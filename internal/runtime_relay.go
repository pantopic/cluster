package internal

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/pantopic/ext-cluster/host-wazero/pb"
	"github.com/pantopic/ext-relay/host-wazero"
	"github.com/pantopic/wazero-pool"
)

type RuntimeRelay interface {
	Register(name string, wasm []byte)
	Start(name, mod string, term uint64, shard Shard) error
	Stop(name, mod string, term uint64, shard Shard) error
	Reload(name, mod string, term uint64, shard Shard) error
}

type runtimeRelay struct {
	ctx      context.Context
	ctxCopy  []ContextCopy
	ext      map[string]Extension
	extRelay wazero_relay.Manager
	pools    map[string]wazeropool.Instance
	mutex    sync.RWMutex
	poolCtx  map[string]context.Context
	wasm     map[string][]byte
	wazero   wazero.Runtime
}

func NewRuntimeRelay(ctx context.Context, a *Agent, ext ...Extension) (r *runtimeRelay) {
	r = &runtimeRelay{
		ctx:     ctx,
		ext:     make(map[string]Extension),
		pools:   make(map[string]wazeropool.Instance),
		poolCtx: make(map[string]context.Context),
		wasm:    make(map[string][]byte),
	}
	r.wazero = wazero.NewRuntimeWithConfig(r.ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(r.ctx, r.wazero)
	for _, e := range ext {
		r.ext[e.Name()] = e
		if err := e.Register(r.ctx, r.wazero); err != nil {
			panic(err)
		}
		r.ctxCopy = append(r.ctxCopy, e.ContextCopy)
	}
	return
}

func (r *runtimeRelay) Register(name string, wasm []byte) {
	cfg := wazero.NewModuleConfig().WithStdout(os.Stdout)
	pool, err := wazeropool.New(r.ctx, r.wazero, wasm,
		wazeropool.WithModuleConfig(cfg),
		wazeropool.WithLimit(runtime.NumCPU()))
	if err != nil {
		panic(err)
	}
	ctx := wazeropool.ContextSet(r.ctx, pool)
	pool.Run(func(mod api.Module) {
		for _, m := range r.ext {
			if ctx, err = m.InitContext(ctx, mod); err != nil {
				panic(err)
			}
		}
	})
	r.pools[name] = pool
	r.poolCtx[name] = ctx
}

func (r *runtimeRelay) Start(name, mod string, term uint64, shard Shard) (err error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	pool, ok := r.pools[mod]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, mod)
	}
	poolCtx, ok := r.poolCtx[mod]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, mod)
	}
	return r.extRelay.Start(poolCtx, pool, term, shardToPb(shard))
}

func (r *runtimeRelay) Stop(name, mod string, term uint64, shard Shard) (err error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	pool, ok := r.pools[mod]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, mod)
	}
	poolCtx, ok := r.poolCtx[mod]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, mod)
	}
	return r.extRelay.Stop(poolCtx, pool, term, shardToPb(shard))
}

func (r *runtimeRelay) Reload(name, mod string, term uint64, shard Shard) (err error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	pool, ok := r.pools[mod]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, mod)
	}
	poolCtx, ok := r.poolCtx[mod]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, mod)
	}
	return r.extRelay.Reload(poolCtx, pool, term, shardToPb(shard))
}

func shardToPb(s Shard) *pb.Shard {
	return &pb.Shard{
		Id:      s.ID,
		Created: s.Created,
		Updated: s.Updated,
		Leader:  s.Leader,
		Term:    s.Term,
		Type:    s.Type,
		Status:  string(s.Status),
		Name:    s.Name,
		Tags:    s.Tags,
	}
}
