package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"

	"github.com/pantopic/microstack/internal/modulestore"
)

type ControllerCluster interface {
	zongzi.RaftEventListener
	SetAgent(*Agent)
	Start(context.Context) error
	Stop()
}

type clusterController struct {
	agent     *Agent
	clock     clock.Clock
	controls  ClusterControls
	ctx       context.Context
	ctxCancel context.CancelFunc
	index     uint64
	isLeader  atomic.Bool
	replicaID atomic.Uint64
	log       Logger
	mutex     sync.RWMutex
	wg        sync.WaitGroup
}

func NewControllerCluster(logger Logger) *clusterController {
	return &clusterController{
		clock: clock.New(),
		log:   logger,
	}
}

func (c *clusterController) SetAgent(agent *Agent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.agent = agent
	c.controls = &clusterControls{
		client: agent.Client(ShardID, zongzi.WithWriteToLeader()),
	}
}

func (c *clusterController) Start(ctx context.Context) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.ctx, c.ctxCancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := c.clock.Ticker(time.Second)
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

func (c *clusterController) shard() (shard Shard) {
	shard, err := c.agent.ShardFind(c.ctx, ShardID)
	if err != nil {
		panic(err)
	}
	return
}

func (c *clusterController) tick() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var err error
	var hadErr bool
	var index uint64
	var updated = true
	if c.isLeader.Load() {
		for updated {
			updated = false
			// controller manager resolves resources
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

func (c *clusterController) LeaderUpdated(info LeaderInfo) {
	if info.ShardID == zongzi.ShardID && info.LeaderID == info.ReplicaID && c.shard().ID == 0 {
		go c.createShard()
	}
	if info.ShardID == ShardID {
		c.log.Infof("[%05d:%05d] LeaderUpdated: %05d (term %d)", info.ShardID, info.ReplicaID, info.LeaderID, info.Term)
		c.isLeader.Store(info.LeaderID == info.ReplicaID)
		c.replicaID.Store(info.ReplicaID)
		if c.isLeader.Load() {
			if !c.controls.NamespaceExists(c.ctx, "internal") {
				go c.init()
			}
		}
	}
}

func (c *clusterController) createShard() {
	c.log.Infof(`Creating shard`)
	if _, _, err := c.agent.ShardCreate(c.ctx, StateMachineUri,
		zongzi.WithName("microstack"),
		zongzi.WithPlacementMembers(3, "microstack:host"),
		zongzi.WithPlacementCover("microstack:host"),
	); err != nil {
		panic(err)
	}
}

func (c *clusterController) init() {
	internal := c.controls.NamespaceCreate(c.ctx, `internal`)
	c.controls.NamespaceCreate(c.ctx, `default`)
	c.controls.ResourceCreate(c.ctx, Resource{
		Name:        `modules`,
		NamespaceID: internal.ID,
		Type:        modulestore.ResourceUri,
	})
	// modules
	// metrics
	// permissions
}

func (c *clusterController) Stop() {
	if c.ctxCancel == nil {
		c.log.Infof(`Cluster controller not running`)
		return
	}
	c.ctxCancel()
	c.wg.Wait()
	c.index = 0
	c.log.Infof(`Cluster controller stopped`)
}
