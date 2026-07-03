package internal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pantopic/cluster/pb"

	"github.com/logbn/zongzi"
)

// controllerService starts and stops services.
type controllerService struct {
	agent     *Agent
	cluster   string
	ctx       context.Context
	ctxCancel context.CancelFunc
	done      chan bool
	index     uint64
	log       Logger
	mutex     sync.RWMutex
	running   map[string]*pb.Service
	state     *State
}

func NewControllerService(a *Agent) *controllerService {
	return &controllerService{agent: a}
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
				c.log.Errorf("[%s] Controller Service: %v", c.agent.HostID(), err)
			}
		}
	}()
	c.mutex.Unlock()
	return c.tick()
}

func (c *controllerService) tick() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	state := c.state.withTxn(false)
	for svc := range state.ServiceIterUpdatedAfter(c.index) {
		if svc.Cluster != c.agent.ClusterName() {
			c.index = svc.Updated
			continue
		}
		if c.svcRunning(svc) {
			if svc.Status == Status_Active {
				if err = c.svcUpdate(svc); err != nil {
					err = fmt.Errorf(`Failed to Update: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			} else {
				if err = c.svcStop(svc); err != nil {
					err = fmt.Errorf(`Failed to Stop: %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			}
		} else if c.svcMatch(svc) {
			if svc.Status == Status_Active {
				if err = c.svcStart(svc); err != nil {
					err = fmt.Errorf(`Failed to Start:  %s (%d): %w`, svcId(svc), svc.Port, err)
					break
				}
			}
		}
		c.index = svc.Updated
	}
	return
}

func svcId(svc *pb.Service) string {
	return fmt.Sprintf("%s.%s.%s", svc.Namespace, svc.Resource, svc.Name)
}

func (c *controllerService) svcStop(svc *pb.Service) (err error) {

	return
}

func (c *controllerService) svcStart(svc *pb.Service) (err error) {

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
	c.log.Infof(`[%s] Matching Service: %s (%d)`, c.agent.HostID(), id, svc.Port)
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
