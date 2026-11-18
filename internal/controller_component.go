package internal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

type ControllerComponent interface {
	SetState(*State)
	Start(ctx context.Context) error
	Stop()
}

// controllerComponent starts and stops services.
type controllerComponent struct {
	agent     *Agent
	cluster   string
	ctx       context.Context
	ctxCancel context.CancelFunc
	done      chan bool
	index     uint64
	isLeader  atomic.Bool
	log       Logger
	mutex     sync.RWMutex
	runtime   *runtimeControl
	state     *State
}

var _ ControllerComponent = new(controllerComponent)

func NewControllerComponent(log Logger, a *Agent, runtime *runtimeControl) *controllerComponent {
	return &controllerComponent{
		agent:   a,
		runtime: runtime,
		log:     log,
	}
}

func (c *controllerComponent) Runtime() *runtimeControl {
	return c.runtime
}

func (c *controllerComponent) SetState(s *State) {
	c.state = s
}

func (c *controllerComponent) Start(ctx context.Context) (err error) {
	defer c.agent.AddRaftEventListener(c)
	c.ctx = ctx
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
				c.log.Errorf("%s Controller Component: %v", c.agent.HostID(), err)
			}
		}
	}()
	c.mutex.Unlock()
	return c.tick()
}

func (c *controllerComponent) updateComponentStatus(client zongzi.ShardClient, item *pb.Component, status Status) {
	item.Status = status
	var data []byte
	var err error
	data, err = proto.Marshal(item)
	if err != nil {
		c.log.Warningf("Error serializing component %v %s", item, err.Error())
		return
	}
	_, _, err = client.Apply(c.ctx, append(data, CMD_COMPONENT_PUT))
	if err != nil {
		c.log.Warningf("Error updating component status %v %s", item, err.Error())
	}
}

func (c *controllerComponent) tick() (err error) {
	if c.state == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var hadErr bool
	var index uint64
	if c.isLeader.Load() {
		client := c.agent.Client(ShardID)
		controls := NewClusterControls(client)
		for {
			state := c.state.withTxn(false)
			index = state.Index()
			for item := range state.ComponentIterUpdatedAfter(c.index) {
				res, ok := state.ResourceFindByName(item.Namespace, item.Resource)
				if !ok {
					return fmt.Errorf("resource not found for component %#v", item)
				}
				controls.updated = false
				var status uint32

				c.log.Debugf("%s Reconciling %s.%s.%s %d", c.agent.HostID(), item.Namespace, item.Resource, item.Name, index)
				status, err = c.runtime.Reconcile(c.ctx, state, res, item, controls)
				if err != nil {
					hadErr = true
					c.log.Warningf("Error resolving component %v %s", item, err.Error())
					continue
				}
				if status == 0 && item.Status != Status_Pending {
					c.log.Infof(`%s Setting component status %s for %s.%s.%s`, c.agent.HostID(), Status_Pending, item.Namespace, item.Resource, item.Name)
					c.updateComponentStatus(client, item, Status_Pending)
					controls.updated = true
				} else if status == 1 && item.Status != Status_Active {
					c.log.Infof(`%s Setting component status %s for %s.%s.%s`, c.agent.HostID(), Status_Active, item.Namespace, item.Resource, item.Name)
					c.updateComponentStatus(client, item, Status_Active)
					controls.updated = true
				}
				if controls.updated {
					break
				}
			}
			if err != nil {
				hadErr = true
			}
			if !controls.updated {
				break
			}
		}
	}
	if !hadErr && index > c.index {
		c.log.Debugf("%s Finished component controller processing %d", c.agent.HostID(), index)
		c.index = index
	}
	return
}

func (c *controllerComponent) LeaderUpdated(info LeaderInfo) {
	if info.ShardID == ShardID {
		c.isLeader.Store(info.LeaderID == info.ReplicaID)
	}
}

func (c *controllerComponent) Stop() {
	defer c.log.Infof(`Stopped Controller Component`)
	if c.ctxCancel != nil {
		c.ctxCancel()
	}
	<-c.done
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}
