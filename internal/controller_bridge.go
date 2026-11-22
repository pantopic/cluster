package internal

import (
	"context"
	"sync"

	"github.com/logbn/zongzi"
)

type ControllerBridge interface {
	SetState(*State)
	Start(ctx context.Context) error
	Stop()
}

// controllerBridge starts and stops bridges.
type controllerBridge struct {
	zongzi.RaftEventListener
	agent   *Agent
	cancel  context.CancelFunc
	cluster string
	ctx     context.Context
	done    bool
	index   uint64
	log     Logger
	mutex   sync.RWMutex
	running map[uint64]map[string]string
	runtime *runtimeBridge
	state   *State
	wg      sync.WaitGroup
	work    chan LeaderInfo
}

var _ ControllerBridge = new(controllerBridge)

func NewControllerBridge(log Logger, a *Agent, runtime *runtimeBridge) *controllerBridge {
	ctx, cancel := context.WithCancel(context.Background())
	return &controllerBridge{
		agent:   a,
		ctx:     ctx,
		cancel:  cancel,
		log:     log,
		runtime: runtime,
		running: make(map[uint64]map[string]string),
		work:    make(chan LeaderInfo, 32),
	}
}

func (c *controllerBridge) Runtime() *runtimeBridge {
	return c.runtime
}

func (c *controllerBridge) SetState(s *State) {
	c.state = s
}

func (c *controllerBridge) Start(ctx context.Context) (err error) {
	c.done = false
	c.agent.RaftEventListenerAdd(c)
	c.wg.Go(func() {
		for {
			select {
			case info := <-c.work:
				c.runLeaderUpdated(info)
			case <-c.ctx.Done():
				c.agent.RaftEventListenerRemove(c)
				c.mutex.Lock()
				c.done = true
				c.mutex.Unlock()
				return
			}
		}
	})
	c.log.Infof(`%s Controller Started: bridge`, c.agent.HostID())
	return
}

func (c *controllerBridge) LeaderUpdated(info LeaderInfo) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.done {
		return
	}
	c.work <- info
}

func (c *controllerBridge) runLeaderUpdated(info LeaderInfo) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	shard, err := c.agent.ShardFind(context.Background(), info.ShardID)
	if err != nil {
		panic(err)
	}
	var prefix = "pantopic:bridge"
	var mods = make(map[string]string)
	for name, mod := range shard.TagsPrefixed(prefix) {
		mods[name[len(prefix)+1:]] = mod
	}
	if len(mods) == 0 {
		return
	}
	running := c.running[shard.ID]
	if running == nil {
		running = make(map[string]string)
	}
	if info.LeaderID == info.ReplicaID {
		for name, found := range running {
			if mods[name] != found {
				c.runtime.Stop(name, found, info.Term, shard)
				delete(running, name)
			}
		}
		for name, mod := range mods {
			if _, ok := running[name]; !ok {
				c.runtime.Start(name, mod, info.Term, shard)
				running[name] = mod
			}
		}
		c.running[shard.ID] = running
	} else if len(running) > 0 {
		for name, mod := range running {
			c.runtime.Stop(name, mod, info.Term, shard)
		}
		delete(c.running, shard.ID)
	}
}

func (c *controllerBridge) Stop() {
	defer c.log.Infof(`%s Controller Stopped: bridge`, c.agent.HostID())
	c.cancel()
	c.wg.Wait()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// for shardID, running := range c.running {
	// 	println(`stopping shard `, shardID)
	// 	shard, err := c.agent.ShardFind(context.Background(), shardID)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	for name, found := range running {
	// 		println(`stopping shard `, name)
	// 		c.runtime.Stop(name, found, 0, shard)
	// 	}
	// }
	// c.running = make(map[uint64]map[string]string)
	c.index = 0
}
