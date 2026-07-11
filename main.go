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
	"github.com/pantopic/cluster/internal/modulestore"
	// "github.com/pantopic/wazero-state-machine/host"

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

	ctrlService := internal.NewControllerService(ctx, agent, logger)
	run(ctx, agent, logger, ctrlService)
	ctrlService.Runtime().Register(turbokube.NameServiceGrpc, turbokube.WasmServiceGrpc)
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
	agent.StateMachineRegister(
		modulestore.StateMachinePersistentUri,
		modulestore.StateMachinePersistentFactory(logger))
	// agent.StateMachineRegister(
	// 	wazero_state_machine.Uri,
	// 	wazero_state_machine.Factory(ctx, logger))
	// agent.StateMachineRegister(
	// 	wazero_state_machine.UriPersistent,
	// 	wazero_state_machine.FactoryPersistent(ctx, logger))
	if err := agent.Start(ctx); err != nil {
		panic(err)
	}

	// Start gRPC API Server
	// Load extensions
	// Start interfaces (ie. table http api)
	// Report ready
}
