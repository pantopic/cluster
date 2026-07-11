package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"

	"github.com/pantopic/cluster/pb"
)

var version = `1`

type ControllerCluster interface {
	zongzi.RaftEventListener
	Start(context.Context, *Agent) error
	Stop()
}

type controllerCluster struct {
	agent     *Agent
	clock     clock.Clock
	cluster   *pb.Cluster
	controls  *ClusterControls
	ctx       context.Context
	ctxCancel context.CancelFunc
	index     uint64
	isLeader  atomic.Bool
	log       Logger
	mutex     sync.RWMutex
	replicaID atomic.Uint64
	wg        sync.WaitGroup
}

var _ ControllerCluster = new(controllerCluster)

func NewControllerCluster(logger Logger) *controllerCluster {
	return &controllerCluster{
		clock: clock.New(),
		log:   logger,
	}
}

func (c *controllerCluster) Start(ctx context.Context, agent *Agent) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.agent = agent
	c.controls = NewClusterControls(
		agent.Client(ShardID, zongzi.WithWriteToLeader()),
	)
	c.ctx, c.ctxCancel = context.WithCancel(ctx)
	c.wg.Go(func() {
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
	})
	return
}

func (c *controllerCluster) shard() (shard Shard) {
	shard, err := c.agent.ShardFind(c.ctx, ShardID)
	if err != nil {
		panic(err)
	}
	return
}

func (c *controllerCluster) tick() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var err error
	var hadErr bool
	var index uint64
	if c.isLeader.Load() {
		for updated := true; updated; {
			updated = false

			// Connect to other clusters and elect lead cluster
			// Run Service Controllers
			// Run Component Controllers

			if err != nil {
				hadErr = true
			}
		}
	}
	if !hadErr && index > c.index {
		c.index = index
	}
}

func (c *controllerCluster) LeaderUpdated(info LeaderInfo) {
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

func (c *controllerCluster) createShard() {
	c.log.Infof(`Creating shard`)
	if _, _, err := c.agent.ShardCreate(c.ctx, StateMachineUri,
		zongzi.WithName("cluster"),
		zongzi.WithPlacementMembers(3, "cluster:member"),
		zongzi.WithPlacementCover("cluster:host"),
	); err != nil {
		panic(err)
	}
}

func (c *controllerCluster) init() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	internal := c.controls.Namespace.FindByName(c.ctx, &pb.Namespace{Name: NAMESPACE_INTERNAL})
	if v, ok := internal.Tags[`version`]; ok && v == version {
		// Internal namespace version presence indicates that the deployment is already initialized
		// In the future, outdated versions may require migration to (for exmaple) add new internal resources
		return
	}
	if internal.Created == 0 {
		internal = c.controls.Namespace.Put(c.ctx, &pb.Namespace{
			Name: NAMESPACE_INTERNAL,
			Tags: map[string]string{`version`: version},
		})
	} else {
		if internal.Tags == nil {
			internal.Tags = make(map[string]string)
		}
		if internal.Tags[`version`] != version {
			internal.Tags[`version`] = version
			internal = c.controls.Namespace.Put(c.ctx, internal)
		}
	}
	ns := c.controls.Namespace.FindByName(c.ctx, &pb.Namespace{Name: NAMESPACE_DEFAULT})
	if ns.Created == 0 {
		ns = c.controls.Namespace.Put(c.ctx, &pb.Namespace{Name: NAMESPACE_DEFAULT})
	}
	c.cluster = c.controls.Cluster.Put(c.ctx, &pb.Cluster{Name: c.agent.ClusterName()})
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
