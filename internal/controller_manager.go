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

type ControllerManager interface {
	SetState(*State)
	Start(ctx context.Context, a *Agent, runtime *runtimeControl) error
	Stop()
}

// controllerManager starts and stops services.
type controllerManager struct {
	agent     *Agent
	client    zongzi.ShardClient
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

var _ ControllerManager = new(controllerManager)

func NewControllerManager(log Logger) *controllerManager {
	return &controllerManager{
		log: log,
	}
}

func (c *controllerManager) Runtime() *runtimeControl {
	return c.runtime
}

func (c *controllerManager) SetState(s *State) {
	c.state = s
}

func (c *controllerManager) Start(ctx context.Context, a *Agent, runtime *runtimeControl) (err error) {
	c.ctx = ctx
	c.agent = a
	c.client = a.Client(ShardID)
	c.runtime = runtime
	fmt.Printf("[%s] Starting controller manager\n", c.agent.HostID())
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
				c.log.Errorf("[%s] Controller Manager: %v", c.agent.HostID(), err)
			}
		}
	}()
	c.mutex.Unlock()
	return c.tick()
}
func (c *controllerManager) updateComponentStatus(item *pb.Component, status Status) {
	item.Status = status
	var data []byte
	var err error
	data, err = proto.Marshal(item)
	if err != nil {
		c.log.Warningf("Error serializing component %v %s", item, err.Error())
		return
	}
	_, _, err = c.client.Apply(c.ctx, append(data, CMD_COMPONENT_PUT))
	if err != nil {
		c.log.Warningf("Error updating component status %v %s", item, err.Error())
	}
}

func (c *controllerManager) tick() (err error) {
	if c.state == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var hadErr bool
	var index uint64
	var controls = NewClusterControls(c.agent.Client(ShardID))
	if c.isLeader.Load() {
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
				status, err = c.runtime.Reconcile(c.ctx, state, res, item, controls)
				if err != nil {
					hadErr = true
					c.log.Warningf("Error resolving component %v %s", item, err.Error())
					continue
				}
				if status == 0 && item.Status != Status_Pending {
					c.updateComponentStatus(item, Status_Pending)
					controls.updated = true
				} else if status == 1 && item.Status != Status_Active {
					c.updateComponentStatus(item, Status_Active)
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

func (c *controllerManager) LeaderUpdated(info LeaderInfo) {
	c.log.Infof("[%05d:%05d] LeaderUpdated: %05d (term %d)", info.ShardID, info.ReplicaID, info.LeaderID, info.Term)
	if info.ShardID == ShardID {
		c.isLeader.Store(info.LeaderID == info.ReplicaID)
	}
}

func (c *controllerManager) Stop() {
	defer c.log.Infof(`Stopped Controller Manager`)
	if c.ctxCancel != nil {
		c.ctxCancel()
	}
	<-c.done
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}
