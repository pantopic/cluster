package internal

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/pantopic/ext-cluster/host-wazero"
	"github.com/pantopic/wazero-pool"

	"github.com/pantopic/cluster/pb"
)

type RuntimeService interface {
	Register(name string, wasm []byte)
	Start(res *pb.Resource, svc *pb.Service) (err error)
	Stop(res *pb.Resource, svc *pb.Service) (err error)
}

type runtimeService struct {
	ctx     context.Context
	ctxCopy []ContextCopy
	ext     map[string]Extension
	lis     map[string]net.Listener
	pools   map[string]wazeropool.Instance
	poolCtx map[string]context.Context
	svc     map[string]ServiceProvider
	wasm    map[string][]byte
	wazero  wazero.Runtime
}

func NewRuntimeService(ctx context.Context, ext ...Extension) (r *runtimeService) {
	r = &runtimeService{
		ctx:     ctx,
		ext:     make(map[string]Extension),
		lis:     make(map[string]net.Listener),
		pools:   make(map[string]wazeropool.Instance),
		poolCtx: make(map[string]context.Context),
		svc:     make(map[string]ServiceProvider),
		wasm:    make(map[string][]byte),
	}
	r.wazero = wazero.NewRuntimeWithConfig(r.ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(r.ctx, r.wazero)
	for _, e := range ext {
		r.ext[e.Name()] = e
		if err := e.Register(r.ctx, r.wazero); err != nil {
			panic(err)
		}
		if sp, ok := e.(ServiceProvider); ok {
			r.svc[sp.ServiceType()] = sp
		}
		r.ctxCopy = append(r.ctxCopy, e.ContextCopy)
	}
	return
}

func (r *runtimeService) Register(name string, wasm []byte) {
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

func (r *runtimeService) Start(res *pb.Resource, svc *pb.Service) (err error) {
	modName := fmt.Sprintf("%s/service/%s", res.Type, svc.Type)
	pool, ok := r.pools[modName]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, modName)
	}
	ctx, ok := r.poolCtx[modName]
	if !ok {
		return fmt.Errorf(`Unrecognized service module %s`, modName)
	}
	sp, ok := r.svc[svc.Type]
	if !ok {
		return fmt.Errorf(`Unrecognized service provider %s`, svc.Type)
	}
	svcName := fmt.Sprintf("%s.%s.%s", res.Namespace, res.Name, svc.Name)
	ctxCopy := append(r.ctxCopy, wazero_cluster.NewResolver(res.Namespace, res.Name))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", svc.Port))
	if err != nil {
		return fmt.Errorf(`Unable to start listener for service %s: %w`, svcName, err)
	}
	r.lis[svcName] = lis
	err = sp.ServerStart(ctx, lis, "", "", pool, ctxCopy...)
	return
}

func (r *runtimeService) Stop(res *pb.Resource, svc *pb.Service) (err error) {
	sp, ok := r.svc[svc.Type]
	if !ok {
		return fmt.Errorf(`Unrecognized service provider %s`, svc.Type)
	}
	svcName := fmt.Sprintf("%s.%s.%s", res.Namespace, res.Name, svc.Name)
	lis, ok := r.lis[svcName]
	if !ok {
		return fmt.Errorf(`Unrecognized listener for service %s`, svcName)
	}
	err = sp.ServerStop(r.ctx, lis)
	if err != nil {
		return
	}
	err = lis.Close()
	delete(r.lis, svcName)
	return
}
