package internal

import (
	"context"
	"sync"
	"time"
)

// The controllerService starts and stops replicas based on replica config.
type controllerService struct {
	agent     *Agent
	ctx       context.Context
	ctxCancel context.CancelFunc
	done      chan bool
	index     uint64
	log       Logger
	mutex     sync.RWMutex
	state     *State
}

func NewControllerService(a *Agent) *controllerService {
	return &controllerService{
		agent: a,
	}
}

func (c *controllerService) Start() (err error) {
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
				c.log.Errorf("Controller Service: %v", err)
			}
		}
	}()
	c.mutex.Unlock()
	return c.tick()
}

func (c *controllerService) tick() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var hadErr bool
	var index uint64
	state := c.state.withTxn(false)
	for s := range state.ServiceIterUpdatedAfter(c.index) {
		c.log.Infof(`Reconcile Service: %s.%s.%s (%d)`, s.Namespace, s.Resource, s.Name, s.Port)
		// start service if match s
		index = s.Updated
	}
	if index <= c.index || hadErr {
		err = nil
		return
	}
	c.index = index
	return
}

func (c *controllerService) Stop() {
	defer c.log.Infof(`Stopped Controller Service`)
	if c.ctxCancel != nil {
		c.ctxCancel()
	}
	<-c.done
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}
