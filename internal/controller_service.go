package internal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

type ControllerService interface {
	SetState(*State)
	Start() error
	Stop()
}

// controllerService starts and stops services.
type controllerService struct {
	agent     *Agent
	client    zongzi.ShardClient
	cluster   string
	ctx       context.Context
	ctxCancel context.CancelFunc
	done      chan bool
	index     uint64
	log       Logger
	mutex     sync.RWMutex
	running   map[string]*pb.Service
	runtime   *runtimeService
	state     *State
}

var _ ControllerService = new(controllerService)

func NewControllerService(ctx context.Context, a *Agent, log Logger, runtime *runtimeService) *controllerService {
	return &controllerService{
		agent:   a,
		client:  a.Client(ShardID),
		ctx:     ctx,
		log:     log,
		runtime: runtime,
		running: make(map[string]*pb.Service),
	}
}

func (c *controllerService) Runtime() *runtimeService {
	return c.runtime
}

func (c *controllerService) SetState(s *State) {
	c.state = s
}

func (c *controllerService) Start() (err error) {
	c.mutex.Lock()
	c.ctx, c.ctxCancel = context.WithCancel(context.Background())
	c.done = make(chan bool)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err = c.tick()
			case <-c.ctx.Done():
				close(c.done)
				return
			}
			if err != nil {
				c.log.Errorf("[%s] Service Controller: %v", c.agent.HostID(), err)
			}
		}
	}()
	c.mutex.Unlock()
	return c.tick()
}

func (c *controllerService) tick() (err error) {
	if c.state == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	state := c.state.withTxn(false)
	for svc := range state.ServiceIterUpdatedAfter(c.index) {
		res, _ := state.ResourceFindByName(svc.Namespace, svc.Resource)
		if c.svcRunning(svc) {
			if svc.Status == Status_Active {
				c.log.Infof(`[%s] Service Controller: Updating %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcUpdate(svc); err != nil {
					err = fmt.Errorf(`Failed to Update: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			} else {
				c.log.Infof(`[%s] Service Controller: Stopping %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcStop(res, svc); err != nil {
					err = fmt.Errorf(`Failed to Stop: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			}
		} else if c.svcMatch(svc) {
			if svc.Status == Status_Active {
				c.log.Infof(`[%s] Service Controller: Starting %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcStart(res, svc); err != nil {
					err = fmt.Errorf(`Failed to Start: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
				c.running[svcId(svc)] = svc
			}
		}
		c.index = svc.Updated
	}
	return
}

func (c *controllerService) svcStop(res *pb.Resource, svc *pb.Service) (err error) {
	err = c.runtime.Stop(res, svc)
	return
}

func (c *controllerService) svcStart(res *pb.Resource, svc *pb.Service) (err error) {
	err = c.runtime.Start(res, svc)
	if err != nil {
		return
	}
	svc.Status = Status_Active
	data, err := proto.Marshal(svc)
	if err != nil {
		return
	}
	_, _, err = c.client.Apply(c.ctx, append(data, CMD_SERVICE_PUT))
	return
}

func (c *controllerService) svcUpdate(svc *pb.Service) (err error) {

	return
}

func (c *controllerService) svcMatch(svc *pb.Service) bool {
	var id = svcId(svc)
	var ok bool
	var host zongzi.Host
	if err := c.agent.State(c.ctx, func(state *zongzi.State) {
		host, ok = state.Host(c.agent.HostID())
	}); err != nil {
		c.log.Panicf(`[%s] Failed to read cluster state: %s `, c.agent.HostID(), err.Error())
	} else if !ok {
		c.log.Panicf(`[%s] Host not found`, c.agent.HostID())
	}
	c.log.Debugf(`[%s] Matching Service: %s (%d)`, c.agent.HostID(), id, svc.Port)
	tagCover, ok := svc.Tags["placement:cover"]
	if !ok || len(tagCover) < 1 {
		return false
	}
	// Service cover tag must match host tags (with AND semantics)
	for tag := range strings.SplitSeq(tagCover, ",") {
		parts := strings.Split(tag, "=")
		if len(parts) != 2 {
			c.log.Infof(`[%s] Malformed Service Cover Tag (%s): "%s"`, c.agent.HostID(), id, tag)
			return false
		}
		if v, ok := host.Tags[parts[0]]; !ok || v != parts[1] {
			return false
		}
	}
	return true
}

func (c *controllerService) svcRunning(svc *pb.Service) bool {
	_, ok := c.running[svcId(svc)]
	return ok
}

func (c *controllerService) Stop() {
	defer c.log.Infof(`Stopped Controller Service`)
	if c.ctxCancel != nil {
		c.ctxCancel()
	}
	<-c.done
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}

func svcId(svc *pb.Service) string {
	return fmt.Sprintf("%s.%s.%s", svc.Namespace, svc.Resource, svc.Name)
}
