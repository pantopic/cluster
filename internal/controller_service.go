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
	cancel  context.CancelFunc
	client  zongzi.ShardClient
	cluster string
	ctx     context.Context
	index   uint64
	log     Logger
	mutex   sync.RWMutex
	running map[string]*pb.Service
	runtime *runtimeService
	state   *State
	wg      sync.WaitGroup
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
	defer c.log.Infof(`%s Controller Started: service`, c.agent.HostID())
	c.mutex.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.wg.Go(func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err = c.tick()
			case <-c.ctx.Done():
				return
			}
			if err != nil {
				c.log.Errorf("%s Service Controller: %v", c.agent.HostID(), err)
			}
		}
	})
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
	for svc := range state.ServiceIterUpdatedAfter(c.index) { // TODO - filter by cluster
		res, _ := state.ResourceFindByName(svc.Namespace, svc.Resource)
		if c.svcRunning(svc) {
			if svc.Status == Status_Active {
				c.log.Debugf(`%s Service Controller: Updating %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcUpdate(res, svc); err != nil {
					err = fmt.Errorf(`Failed to Update: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			} else {
				c.log.Infof(`%s Service Controller: Stopping %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcStop(res, svc); err != nil {
					err = fmt.Errorf(`Failed to Stop: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			}
		} else if c.svcMatch(svc) {
			if svc.Status == Status_Active {
				c.log.Infof(`%s Service Controller: Starting %s (%d)`, c.agent.HostID(), svcId(svc), svc.Port)
				if err = c.svcStart(res, svc); err != nil {
					err = fmt.Errorf(`Failed to Start: %s (%d): %w`, svcId(svc), svc.Port, err)
					continue
					// break
				}
			}
		}
		c.index = svc.Updated
	}
	return
}

func (c *controllerService) svcStop(res *pb.Resource, svc *pb.Service) (err error) {
	defer delete(c.running, svcId(svc))
	return c.runtime.Stop(svc)
}

func (c *controllerService) svcStart(res *pb.Resource, svc *pb.Service) (err error) {
	id := svcId(svc)
	if _, ok := c.running[id]; ok {
		return
	}
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
	if err != nil {
		c.runtime.Stop(svc)
		return
	}
	c.running[id] = svc
	return
}

func (c *controllerService) svcUpdate(res *pb.Resource, svc *pb.Service) (err error) {
	id := svcId(svc)
	if _, ok := c.running[id]; !ok {
		return
	}
	return c.runtime.Update(res, svc)
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
	defer c.log.Infof(`%s Controller Stopped: service`, c.agent.HostID())
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for id, svc := range c.running {
		c.runtime.Stop(svc)
		delete(c.running, id)
	}
	c.index = 0
}

func svcId(svc *pb.Service) string {
	return fmt.Sprintf("%s.%s.%s", svc.Namespace, svc.Resource, svc.Name)
}
