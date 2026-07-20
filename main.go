package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/logbn/zongzi"
	"github.com/pantopic/cluster/internal"

	"github.com/pantopic/wazero-atomic/host"
	"github.com/pantopic/wazero-buffer-pool/host"
	"github.com/pantopic/wazero-global/host"
	"github.com/pantopic/wazero-grpc-server/host"
	"github.com/pantopic/wazero-lmdb/host"
	"github.com/pantopic/wazero-range-watch/host"
	"github.com/pantopic/wazero-shard-client/host"
	"github.com/pantopic/wazero-small-cache/host"
	"github.com/pantopic/wazero-state-machine/host"

	"github.com/pantopic/turbokube/embed"
)

func main() {
	ctx := context.Background()
	cfg := getConfig()
	logger := getLogger(cfg)
	ctrl := internal.NewControllerCluster(logger)
	opts := []zongzi.AgentOption{
		zongzi.WithDirRaft(cfg.DirRaft),
		zongzi.WithDirWAL(cfg.DirWAL),
		zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortZongzi)),
		zongzi.WithRaftEventListener(ctrl),
	}
	if s := os.Getenv("PANTOPIC_HOST_TAGS"); s != "" {
		opts = append(opts, zongzi.WithHostTags(strings.Split(s, "|")...))
	}
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","), opts...)
	if err != nil {
		panic(err)
	}
	ctrl.Start(ctx, agent)

	runtimeSvc := internal.NewRuntimeService(ctx, []internal.Extension{
		wazero_global.New(),
		wazero_grpc_server.New(),
		wazero_buffer_pool.New(),
		wazero_shard_client.New(agent),
	}...)
	runtimeSvc.Register(turbokube.ServiceGrpcName, turbokube.ServiceGrpcWasm)

	extLmdb := wazero_lmdb.New()
	runtimeStorage := internal.NewRuntimeStorage(ctx, agent, []internal.Extension{
		extLmdb,
		wazero_global.New(),
		wazero_atomic.New(),
		wazero_range_watch.New(),
		wazero_small_cache.New(),
		wazero_state_machine.New(
			wazero_state_machine.WithStorageExtensionPersistent(extLmdb),
		),
	}...)
	runtimeStorage.Register(agent, turbokube.StorageKvName, turbokube.StorageKvWasm)

	ctrlService := internal.NewControllerService(ctx, agent, logger, runtimeSvc)
	run(ctx, agent, logger, ctrlService)
	ctrlService.Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop

	ctrl.Stop()
	agent.Stop()
}

func run(ctx context.Context, agent *zongzi.Agent, logger zongzi.Logger, ctrlService internal.ControllerService) {
	agent.StateMachineRegister(
		internal.StateMachineUri,
		internal.StateMachineFactory(logger, ctrlService))
	if err := agent.Start(ctx); err != nil {
		panic(err)
	}
}
