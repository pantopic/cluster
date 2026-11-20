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
	cluster string
	work    chan LeaderInfo
	index   uint64
	log     Logger
	wg      sync.WaitGroup
	mutex   sync.RWMutex
	running map[uint64]map[string]string
	runtime *runtimeBridge
	state   *State
}

var _ ControllerBridge = new(controllerBridge)

func NewControllerBridge(log Logger, a *Agent, runtime *runtimeBridge) *controllerBridge {
	return &controllerBridge{
		agent:   a,
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
	c.agent.AddRaftEventListener(c)
	c.wg.Go(func() {
		for info := range c.work {
			c.runLeaderUpdated(info)
		}
	})
	return
}

func (c *controllerBridge) LeaderUpdated(info LeaderInfo) {
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
	defer c.log.Infof(`Stopped Controller Bridge`)
	close(c.work)
	c.wg.Wait()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}
