package internal

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"
)

type ControllerCluster interface {
	zongzi.RaftEventListener
	SetAgent(*Agent)
	Start(context.Context) error
	Stop()
}

type controllerCluster struct {
	agent     *Agent
	clock     clock.Clock
	ctx       context.Context
	ctxCancel context.CancelFunc
	index     uint64
	isLeader  atomic.Bool
	log       Logger
	mutex     sync.RWMutex
	shard     Shard
	wg        sync.WaitGroup
}

func NewControllerCluster(logger Logger) *controllerCluster {
	return &controllerCluster{
		clock: clock.New(),
		log:   logger,
	}
}

func (c *controllerCluster) SetAgent(agent *Agent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.agent = agent
}

func (c *controllerCluster) Start(ctx context.Context) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.ctx, c.ctxCancel = context.WithCancel(ctx)
	shard, err := c.agent.ShardFind(ctx, ShardID)
	if err != nil {
		panic(err)
	}
	c.log.Infof(`shard: %#v`, shard)
	if shard.ID == 0 {
		c.log.Infof(`Creating shard`)
		shard, _, err = c.agent.ShardCreate(ctx, StateMachineUri,
			zongzi.WithName("microstack"),
			zongzi.WithPlacementMembers(3, "microstack:host"))
		if err != nil {
			panic(err)
		}
		client := c.agent.Client(shard.ID)
		for _, namespace := range []string{"system", "default"} {
			c.log.Infof(`Creating namespace: ` + namespace)
			for {
				b, err := json.Marshal(map[string]any{
					"type": "namespaceCreate",
					"data": Namespace{
						Name: namespace,
					},
				})
				if err != nil {
					panic(err)
				}
				if _, _, err = client.Apply(ctx, b); err != nil {
					c.log.Infof(`%v`, err)
					c.clock.Sleep(time.Second)
					continue
				}
				break
			}
		}
	}
	c.shard = shard
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := c.clock.Ticker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-t.C:
				c.tick()
			}
		}
	}()
	return
}

func (c *controllerCluster) tick() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var err error
	var hadErr bool
	var index uint64
	var updated = true
	if c.isLeader.Load() {
		for updated {
			updated = false
			//
			if err != nil {
				hadErr = true
			}
		}
	}
	if !hadErr && index > c.index {
		c.index = index
	}
	return
}

func (c *controllerCluster) LeaderUpdated(info LeaderInfo) {
	if info.ShardID == ShardID {
		c.log.Infof("[%05d:%05d] LeaderUpdated: %05d (term %d)", info.ShardID, info.ReplicaID, info.LeaderID, info.Term)
		c.isLeader.Store(info.LeaderID == info.ReplicaID)
	}
}

func (c *controllerCluster) Stop() {
	if c.ctxCancel == nil {
		c.log.Infof(`Cluster controller not running`)
		return
	}
	c.ctxCancel()
	c.wg.Wait()
	c.index = 0
	c.log.Infof(`Cluster controller stopped`)
}
