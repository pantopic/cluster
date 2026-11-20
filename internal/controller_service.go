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
	Start(ctx context.Context) error
	Stop()
}

// controllerService starts and stops services.
type controllerService struct {
	agent   *Agent
	client  zongzi.ShardClient
	cluster string
	cancel  context.CancelFunc
	done    chan bool
	index   uint64
	log     Logger
	mutex   sync.RWMutex
	running map[string]*pb.Service
	runtime *runtimeService
	state   *State
}

var _ ControllerService = new(controllerService)

func NewControllerService(log Logger, a *Agent, runtime *runtimeService) *controllerService {
	return &controllerService{
		agent:   a,
		client:  a.Client(ShardID),
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

func (c *controllerService) Start(ctx context.Context) (err error) {
	c.mutex.Lock()
	ctx, c.cancel = context.WithCancel(context.Background())
	c.done = make(chan bool)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err = c.tick(ctx)
			case <-ctx.Done():
				close(c.done)
				return
			}
			if err != nil {
				c.log.Errorf("%s Service Controller: %v", c.agent.HostID(), err)
			}
		}
	}()
	c.mutex.Unlock()
	return c.tick(ctx)
}

func (c *controllerService) tick(ctx context.Context) (err error) {
	if c.state == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	state := c.state.withTxn(false)
	for svc := range state.ServiceIterUpdatedAfter(c.index) { // TODO - filter by cluster
		res, _ := state.ResourceFindByName(svc.Namespace, svc.Resource)
		if c.svcRunning(svc) {
			if svc.Status == Status_Active {
				c.log.Debugf(`%s Service Controller: Updating %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcUpdate(ctx, svc); err != nil {
					err = fmt.Errorf(`Failed to Update: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			} else {
				c.log.Infof(`%s Service Controller: Stopping %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcStop(ctx, res, svc); err != nil {
					err = fmt.Errorf(`Failed to Stop: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			}
		} else if c.svcMatch(svc) {
			if svc.Status == Status_Active {
				c.log.Infof(`%s Service Controller: Starting %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcStart(ctx, res, svc); err != nil {
					err = fmt.Errorf(`Failed to Start: %s (%d): %w`, svcId(svc), svc.Port, err)
					continue
					// break
				}
				c.running[svcId(svc)] = svc
			}
		}
		c.index = svc.Updated
	}
	return
}

func (c *controllerService) svcStop(ctx context.Context, res *pb.Resource, svc *pb.Service) (err error) {
	err = c.runtime.Stop(res, svc)
	return
}

func (c *controllerService) svcStart(ctx context.Context, res *pb.Resource, svc *pb.Service) (err error) {
	err = c.runtime.Start(res, svc)
	if err != nil {
		return
	}
	svc.Status = Status_Active
	data, err := proto.Marshal(svc)
	if err != nil {
		return
	}
	_, _, err = c.client.Apply(ctx, append(data, CMD_SERVICE_PUT))
	return
}

func (c *controllerService) svcUpdate(ctx context.Context, svc *pb.Service) (err error) {

	return
}

func (c *controllerService) svcMatch(svc *pb.Service) bool {
	var id = svcId(svc)
	var ok bool
	var host zongzi.Host
	if err := c.agent.State(context.Background(), func(state *zongzi.State) {
		host, ok = state.Host(c.agent.HostID())
	}); err != nil {
		c.log.Panicf(`%s Failed to read cluster state: %s `, c.agent.HostID(), err.Error())
	} else if !ok {
		c.log.Panicf(`%s Host not found`, c.agent.HostID())
	}
	c.log.Debugf(`%s Matching Service: %s (%d)`, c.agent.HostID(), id, svc.Port)
	tagCover, ok := svc.Tags["placement:cover"]
	if !ok || len(tagCover) < 1 {
		return false
	}
	// Service cover tag must match host tags (with AND semantics)
	for tag := range strings.SplitSeq(tagCover, ",") {
		parts := strings.Split(tag, "=")
		if v, ok := host.Tags[parts[0]]; !ok || (len(parts) > 1 && v != parts[1]) {
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
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}

func svcId(svc *pb.Service) string {
	return fmt.Sprintf("%s.%s.%s", svc.Namespace, svc.Resource, svc.Name)
}
