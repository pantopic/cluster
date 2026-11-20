package internal

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/pantopic/ext-bridge/host-wazero"
	"github.com/pantopic/ext-cluster/host-wazero"
	"github.com/pantopic/ext-cluster/host-wazero/pb"
	"github.com/pantopic/wazero-pool"
)

type RuntimeBridge interface {
	Register(name string, wasm []byte)
	Start(name, mod string, term uint64, shard Shard) error
	Stop(name, mod string, term uint64, shard Shard) error
	Reload(name, mod string, term uint64, shard Shard) error
}

type runtimeBridge struct {
	ctx       context.Context
	ctxCopy   []ContextCopy
	ext       map[string]Extension
	extBridge wazero_bridge.Manager
	pools     map[string]wazeropool.Instance
	mutex     sync.RWMutex
	poolCtx   map[string]context.Context
	wasm      map[string][]byte
	wazero    wazero.Runtime
}

func NewRuntimeBridge(ctx context.Context, a *Agent, ext ...Extension) (r *runtimeBridge) {
	r = &runtimeBridge{
		ctx:     ctx,
		ext:     make(map[string]Extension),
		pools:   make(map[string]wazeropool.Instance),
		poolCtx: make(map[string]context.Context),
		wasm:    make(map[string][]byte),
	}
	r.wazero = wazero.NewRuntimeWithConfig(r.ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(r.ctx, r.wazero)
	extBridge := wazero_bridge.New()
	ext = append(ext, extBridge)
	r.extBridge = extBridge
	for _, e := range ext {
		if _, ok := r.ext[e.Name()]; ok {
			continue
		}
		r.ext[e.Name()] = e
		if err := e.Register(r.ctx, r.wazero); err != nil {
			panic(err)
		}
		r.ctxCopy = append(r.ctxCopy, e.ContextCopy)
	}
	return
}

func (r *runtimeBridge) Register(name string, wasm []byte) {
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

func (r *runtimeBridge) Start(name, mod string, term uint64, shard Shard) (err error) {
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
	ctx := wazeropool.ContextSet(r.ctx, pool)
	for _, cc := range r.ctxCopy {
		ctx = cc(ctx, poolCtx)
	}
	parts := strings.Split(shard.Name, ".")
	ctx = wazero_cluster.NewResolver(parts[0], parts[1])(ctx, ctx)
	return r.extBridge.Start(ctx, term, shardToPb(shard))
}

func (r *runtimeBridge) Stop(name, mod string, term uint64, shard Shard) (err error) {
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
	ctx := wazeropool.ContextSet(r.ctx, pool)
	for _, cc := range r.ctxCopy {
		ctx = cc(ctx, poolCtx)
	}
	parts := strings.Split(shard.Name, ".")
	ctx = wazero_cluster.NewResolver(parts[0], parts[1])(ctx, ctx)
	return r.extBridge.Stop(ctx, term, shardToPb(shard))
}

func (r *runtimeBridge) Reload(name, mod string, term uint64, shard Shard) (err error) {
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
	ctx := wazeropool.ContextSet(r.ctx, pool)
	for _, cc := range r.ctxCopy {
		ctx = cc(ctx, poolCtx)
	}
	parts := strings.Split(shard.Name, ".")
	ctx = wazero_cluster.NewResolver(parts[0], parts[1])(ctx, ctx)
	return r.extBridge.Reload(ctx, term, shardToPb(shard))
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
