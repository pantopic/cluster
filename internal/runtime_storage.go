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
	"github.com/pantopic/wazero-pool"

	"github.com/pantopic/ext-mdb/host-wazero-lmdb"
	"github.com/pantopic/ext-raft/host-wazero"
)

type runtimeStorage struct {
	agent  *Agent
	ctx    context.Context
	dir    string
	ext    map[string]Extension
	pools  map[string]wazeropool.Instance
	wasm   map[string][]byte
	wazero wazero.Runtime
}

func NewRuntimeStorage(ctx context.Context, a *Agent, ext ...Extension) (r *runtimeStorage) {
	r = &runtimeStorage{
		agent: a,
		ctx:   ctx,
		ext:   make(map[string]Extension),
		pools: make(map[string]wazeropool.Instance),
		wasm:  make(map[string][]byte),
	}
	r.wazero = wazero.NewRuntimeWithConfig(r.ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(r.ctx, r.wazero)
	var ctxCopy []ContextCopy
	var extStorage []wazero_raft.StorageExtensionPersistent
	for _, e := range ext {
		r.ext[e.Name()] = e
		if err := e.Register(r.ctx, r.wazero); err != nil {
			panic(err)
		}
		ctxCopy = append(ctxCopy, e.ContextCopy)
		if esp, ok := e.(wazero_raft.StorageExtensionPersistent); ok {
			extStorage = append(extStorage, esp)
		}
	}
	ctxInit := func(ctx context.Context, shardID, replicaID uint64) context.Context {
		dir := fmt.Sprintf("%s/data/%d/%d", r.dir, shardID, replicaID)
		return wazero_lmdb.EnvRegisterDir(ctx, dir)
	}
	logger := zongzi.GetLogger(`statemachine`)
	fsm := wazero_raft.FactoryPersistent(ctx, ctxInit, ctxCopy, logger, r.poolFactoryStorage, extStorage...)
	a.StateMachineRegister(wazero_raft.UriPersistent, fsm)
	return
}

func (r *runtimeStorage) Register(agent *Agent, name string, wasm []byte) {
	cfg := wazero.NewModuleConfig().WithStdout(os.Stdout)
	pool, err := wazeropool.New(r.ctx, r.wazero, wasm,
		wazeropool.WithModuleConfig(cfg),
		wazeropool.WithLimit(runtime.NumCPU()))
	if err != nil {
		panic(err)
	}
	pool.Run(func(mod api.Module) {
		for _, m := range r.ext {
			if r.ctx, err = m.InitContext(r.ctx, mod); err != nil {
				panic(err)
			}
		}
	})
	r.pools[name] = pool

}
func (r *runtimeStorage) poolFactoryStorage(shardID uint64) wazeropool.Instance {
	var ok bool
	var shard Shard
	r.agent.StateLocal(func(s *zongzi.State) {
		if shard, ok = s.Shard(shardID); !ok {
			return
		}
	})
	if !ok {
		return nil
	}
	pool, ok := r.pools[shard.Type]
	if !ok {
		return nil
	}
	return pool
}
