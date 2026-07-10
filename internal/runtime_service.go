package internal

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/logbn/zongzi"
	"github.com/soheilhy/cmux"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pantopic/wazero-buffer-pool/host"
	"github.com/pantopic/wazero-global/host"
	"github.com/pantopic/wazero-grpc-server/host"
	"github.com/pantopic/wazero-pool"
	"github.com/pantopic/wazero-shard-client/host"

	"github.com/pantopic/cluster/pb"
)

type extService interface {
	wazero_grpc_server.ContextCopier
	Register(context.Context, wazero.Runtime) error
	InitContext(context.Context, api.Module) (context.Context, error)
}

type runtimeService struct {
	agent   *zongzi.Agent
	ctx     context.Context
	ctxCopy []wazero_grpc_server.ContextCopier
	ext     map[string]extService
	pools   map[string]wazeropool.Instance
	wasm    map[string][]byte
	wazero  wazero.Runtime
}

func NewRuntimeService(ctx context.Context, agent *zongzi.Agent) (r *runtimeService) {
	r = &runtimeService{
		agent: agent,
		ctx:   ctx,
		ext:   make(map[string]extService),
		pools: make(map[string]wazeropool.Instance),
	}
	r.wazero = wazero.NewRuntimeWithConfig(r.ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(r.ctx, r.wazero)
	ext := []struct {
		name string
		ext  extService
	}{
		{"global", wazero_global.New()},
		{"grpc", wazero_grpc_server.New()},
		{"buffer_pool", wazero_buffer_pool.New()},
		{"shard_client", wazero_shard_client.New(r.agent)},
	}
	for _, m := range ext {
		r.ext[m.name] = m.ext
		if err := m.ext.Register(r.ctx, r.wazero); err != nil {
			panic(err)
		}
		r.ctxCopy = append(r.ctxCopy, wazero_grpc_server.ContextCopier(m.ext))
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
	r.ctx = wazeropool.ContextSet(r.ctx, pool)
	pool.Run(func(mod api.Module) {
		for _, m := range r.ext {
			if r.ctx, err = m.InitContext(r.ctx, mod); err != nil {
				panic(err)
			}
		}
	})
	r.pools[name] = pool
}

func (r *runtimeService) Start(res *pb.Resource, svc *pb.Service) (err error) {
	name := fmt.Sprintf("%s/service/%s", res.Type, svc.Type)
	pool, ok := r.pools[name]
	if !ok {
		return fmt.Errorf(`Unrecognized service %s`, name)
	}
	var opts = []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Hour,
			Timeout: 20 * time.Second,
		}),
	}
	var grpcServer = grpc.NewServer(opts...)
	err = r.ext["grpc"].(wazero_grpc_server.ServiceRegistry).RegisterServices(r.ctx, grpcServer, pool,
		append(r.ctxCopy, wazero_shard_client.NewResolver(res.Namespace, res.Name))...)
	if err != nil {
		panic(err)
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", svc.Port))
	if err != nil {
		panic(err)
	}
	m := cmux.New(lis)
	grpcListener := m.Match(cmux.HTTP2())
	httpListener := m.Match(cmux.Any())
	go func() {
		if err = grpcServer.Serve(grpcListener); err != nil {
			panic(err)
		}
	}()
	httpServer := &http.Server{
		Handler: grpcServer,
	}
	go func() {
		if err = httpServer.Serve(httpListener); err != nil {
			panic(err)
		}
	}()
	go func() {
		if err := m.Serve(); err != nil {
			panic(err)
		}
	}()
	return
}
