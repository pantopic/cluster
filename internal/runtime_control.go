package internal

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/logbn/zongzi"
	"github.com/pantopic/ext-cluster/host-wazero"
	"github.com/pantopic/wazero-pool"

	"github.com/pantopic/cluster/pb"
)

type reconcilerExtension interface {
	wazero_cluster.Reconciler
	Extension
}

type RuntimeControl interface {
	Register(name string, wasm []byte)
	Reconcile(ctx context.Context, state *State, res *pb.Resource, item *pb.Component, controls *ClusterControls) (status uint32, err error)
}

type runtimeControl struct {
	ctx        context.Context
	ctxCopy    []ContextCopy
	ext        map[string]Extension
	extCluster reconcilerExtension
	poolCtx    map[string]context.Context
	pools      map[string]wazeropool.Instance
	wasm       map[string][]byte
	wazero     wazero.Runtime
}

func NewRuntimeControl(ctx context.Context, agent *zongzi.Agent, ext ...Extension) (r *runtimeControl) {
	r = &runtimeControl{
		ctx:        ctx,
		ext:        make(map[string]Extension),
		extCluster: wazero_cluster.New(agent),
		poolCtx:    make(map[string]context.Context),
		pools:      make(map[string]wazeropool.Instance),
		wasm:       make(map[string][]byte),
	}
	r.wazero = wazero.NewRuntimeWithConfig(r.ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(r.ctx, r.wazero)
	ext = append(ext, r.extCluster)
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

func (r *runtimeControl) Register(name string, wasm []byte) {
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

func (r *runtimeControl) Reconcile(ctx context.Context, state *State, res *pb.Resource, item *pb.Component, controls *ClusterControls) (status uint32, err error) {
	// Lookup the pool and context for the given component type.
	modName := fmt.Sprintf("%s/control/%s", res.Type, item.Type)
	pool, ok := r.pools[modName]
	if !ok {
		return 0, fmt.Errorf("no pool found for component %s", modName)
	}
	poolCtx, ok := r.poolCtx[modName]
	if !ok {
		return 0, fmt.Errorf("no context found for component %s", modName)
	}
	for _, fn := range r.ctxCopy {
		ctx = fn(ctx, poolCtx)
	}
	return r.extCluster.Reconcile(ctx, pool, extpbResource(res), extpbComponent(item))
}
