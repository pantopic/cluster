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

	"github.com/pantopic/ext-atomic/host-wazero"
	"github.com/pantopic/ext-buffer/host-wazero"
	"github.com/pantopic/ext-global/host-wazero"
	"github.com/pantopic/ext-grpc-server/host-wazero"
	"github.com/pantopic/ext-mdb/host-wazero-lmdb"
	"github.com/pantopic/ext-raft/host-wazero"
	"github.com/pantopic/ext-range-watch/host-wazero"
	"github.com/pantopic/ext-shard-client/host-wazero"
	"github.com/pantopic/ext-small-cache/host-wazero"

	"github.com/pantopic/kitsune/embed"
	"github.com/pantopic/turbokube/embed"
)

func main() {
	ctx := context.Background()
	cfg := getConfig()
	logger := getLogger(cfg)
	ctrlCluster := internal.NewControllerCluster(logger)
	ctrlManager := internal.NewControllerManager(logger)
	opts := []zongzi.AgentOption{
		zongzi.WithDirRaft(cfg.DirRaft),
		zongzi.WithDirWAL(cfg.DirWAL),
		zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortZongzi)),
		zongzi.WithRaftEventListener(ctrlCluster),
	}
	if s := os.Getenv("PANTOPIC_HOST_TAGS"); s != "" {
		opts = append(opts, zongzi.WithHostTags(strings.Split(s, "|")...))
	}

	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","), opts...)
	if err != nil {
		panic(err)
	}
	defer agent.Stop()

	ctrlCluster.Start(ctx, agent)
	defer ctrlCluster.Stop()

	extLmdb := wazero_lmdb.New()
	runtimeService := internal.NewRuntimeService(ctx, []internal.Extension{
		wazero_global.New(),
		wazero_grpc_server.New(),
		wazero_buffer.New(),
		wazero_shard_client.New(agent),
	}...)
	runtimeStorage := internal.NewRuntimeStorage(ctx, agent, []internal.Extension{
		extLmdb,
		wazero_global.New(),
		wazero_atomic.New(),
		wazero_range_watch.New(),
		wazero_small_cache.New(),
		wazero_raft.New(
			wazero_raft.WithStorageExtensionPersistent(extLmdb),
		),
	}...)
	runtimeControl := internal.NewRuntimeControl(ctx, agent, []internal.Extension{
		wazero_shard_client.New(agent),
	}...)

	turbokube.Register(runtimeService, runtimeStorage, runtimeControl)
	kitsune.Register(runtimeService, runtimeStorage, runtimeControl)

	ctrlService := internal.NewControllerService(ctx, agent, logger, runtimeService)

	run(ctx, agent, logger, ctrlService, ctrlManager)

	ctrlService.Start()
	defer ctrlService.Stop()

	ctrlManager.Start(ctx, agent, runtimeControl)
	defer ctrlManager.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop
}

func run(ctx context.Context, agent *zongzi.Agent, logger zongzi.Logger, ctrlService internal.ControllerService, ctrlManager internal.ControllerManager) {
	agent.StateMachineRegister(
		internal.StateMachineUri,
		internal.StateMachineFactory(logger, ctrlService, ctrlManager))
	if err := agent.Start(ctx); err != nil {
		panic(err)
	}
}
