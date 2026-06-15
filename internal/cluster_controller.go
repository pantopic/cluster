package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"

	"github.com/pantopic/cluster/internal/modulestore"
	"github.com/pantopic/cluster/pb"
)

var version = `1`

type ClusterController interface {
	zongzi.RaftEventListener
	Start(context.Context, *Agent) error
	Stop()
}

type clusterController struct {
	agent     *Agent
	clock     clock.Clock
	cluster   *pb.Cluster
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

func (c *clusterController) Start(ctx context.Context, agent *Agent) (err error) {
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
	if c.isLeader.Load() {
		for updated := true; updated; {
			updated = false

			// TODO: schedule service endpoints

			if err != nil {
				hadErr = true
			}
		}
	}
	if !hadErr && index > c.index {
		c.index = index
	}
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	internal := c.controls.Namespace.FindByName(c.ctx, &pb.Namespace{Name: NAMESPACE_INTERNAL})
	if v, ok := internal.Tags[`version`]; ok && v == version {
		return
	}
	if internal.Created == 0 {
		internal = c.controls.Namespace.Put(c.ctx, &pb.Namespace{Name: NAMESPACE_INTERNAL})
	}
	for _, name := range []string{
		`model`,
		`module`,
		`log`,
		`metric`,
		`policy`,
	} {
		if c.controls.Resource.FindByName(c.ctx, &pb.Resource{
			Namespace: internal.Name,
			Name:      name,
			Uri:       modulestore.ResourceUri,
		}).Created == 0 {
			c.log.Infof(`Creating internal %s store`, name)
			c.controls.Resource.Put(c.ctx, &pb.Resource{
				Namespace: internal.Name,
				Name:      name,
				Uri:       modulestore.ResourceUri,
			})
		}
	}
	ns := c.controls.Namespace.FindByName(c.ctx, &pb.Namespace{Name: NAMESPACE_DEFAULT})
	if ns.Created == 0 {
		ns = c.controls.Namespace.Put(c.ctx, &pb.Namespace{Name: NAMESPACE_DEFAULT})
	}
	if ns.Tags == nil {
		ns.Tags = make(map[string]string)
	}
	if ns.Tags[`version`] != version {
		ns.Tags[`version`] = version
		ns = c.controls.Namespace.Put(c.ctx, ns)
	}
	c.cluster = c.controls.Cluster.Put(c.ctx, &pb.Cluster{Name: c.agent.ClusterName()})
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
