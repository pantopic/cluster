package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"

	"github.com/pantopic/cluster/internal/modulestore"
)

var version = `1`

type ClusterController interface {
	zongzi.RaftEventListener
	SetAgent(*Agent)
	Start(context.Context) error
	Stop()
}

type clusterController struct {
	agent     *Agent
	clock     clock.Clock
	controls  *ClusterControls
	ctx       context.Context
	ctxCancel context.CancelFunc
	index     uint64
	isLeader  atomic.Bool
	replicaID atomic.Uint64
	log       Logger
	mutex     sync.RWMutex
	wg        sync.WaitGroup
}

var _ ClusterController = new(clusterController)

func NewClusterController(logger Logger) *clusterController {
	return &clusterController{
		clock: clock.New(),
		log:   logger,
	}
}

func (c *clusterController) SetAgent(agent *Agent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.agent = agent
	c.controls = NewClusterControls(
		agent.Client(ShardID, zongzi.WithWriteToLeader()),
	)
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
		if info.LeaderID == info.ReplicaID {
			go c.init()
		}
	}
}

func (c *clusterController) createShard() {
	c.log.Infof(`Creating shard`)
	if _, _, err := c.agent.ShardCreate(c.ctx, StateMachineUri,
		zongzi.WithName("cluster"),
		zongzi.WithPlacementMembers(3, "cluster:member"),
		zongzi.WithPlacementCover("cluster:host"),
	); err != nil {
		panic(err)
	}
}

func (c *clusterController) init() {
	internal := c.controls.Namespace.FindByName(c.ctx, Namespace{Name: `internal`})
	if v, ok := internal.Tags[`version`]; ok && v == version {
		return
	}
	if internal.Created == 0 {
		internal = c.controls.Namespace.Create(c.ctx, Namespace{Name: `internal`})
	}
	moduleStore := Resource{
		NamespaceID: internal.GetID(),
		Name:        `modules`,
		Uri:         modulestore.ResourceUri,
	}
	if m := c.controls.Resource.FindByName(c.ctx, moduleStore); m.Created > 0 {
		moduleStore = m
	} else {
		moduleStore = c.controls.Resource.Create(c.ctx, moduleStore)
	}
	// Create module store
	// Create log store
	// Create metric store
	// Create permission store
	if c.controls.Namespace.FindByName(c.ctx, Namespace{Name: `default`}).Created == 0 {
		c.controls.Namespace.Create(c.ctx, Namespace{Name: `default`})
	}
	c.controls.Namespace.AddTags(c.ctx, Namespace{ID: internal.ID, Tags: map[string]string{
		`version`: version,
	}})
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
