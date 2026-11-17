package internal

import (
	"context"
	"sync"

	"github.com/logbn/zongzi"
)

type ControllerRelay interface {
	SetState(*State)
	Start(ctx context.Context) error
	Stop()
}

// controllerRelay starts and stops relays.
type controllerRelay struct {
	zongzi.RaftEventListener
	agent   *Agent
	cluster string
	done    chan bool
	index   uint64
	log     Logger
	mutex   sync.RWMutex
	running map[uint64]map[string]string
	runtime *runtimeRelay
	state   *State
}

var _ ControllerRelay = new(controllerRelay)

func NewControllerRelay(log Logger, a *Agent, runtime *runtimeRelay) *controllerRelay {
	return &controllerRelay{
		agent:   a,
		log:     log,
		runtime: runtime,
		running: make(map[uint64]map[string]string),
	}
}

func (c *controllerRelay) Runtime() *runtimeRelay {
	return c.runtime
}

func (c *controllerRelay) SetState(s *State) {
	c.state = s
}

func (c *controllerRelay) Start(ctx context.Context) (err error) {
	c.agent.AddRaftEventListener(c)
	return
}

func (c *controllerRelay) LeaderUpdated(info LeaderInfo) {
	go func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		shard, err := c.agent.ShardFind(context.Background(), info.ShardID)
		if err != nil {
			panic(err)
		}
		var prefix = "pantopic:relay"
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
	}()
}

func (c *controllerRelay) Stop() {
	defer c.log.Infof(`Stopped Controller Relay`)
	<-c.done
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}
