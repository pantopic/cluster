package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/logbn/zongzi"

	"github.com/pantopic/microstack/guest"
	"github.com/pantopic/microstack/internal"
	"github.com/pantopic/microstack/internal/modulestore"
	"github.com/pantopic/microstack/system"
)

func main() {
	cfg := getConfig()
	ctx := context.Background()
	logger := getLogger(cfg)
	ctrl := internal.NewControllerCluster(logger)
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithDirRaft(cfg.DirRaft),
		zongzi.WithDirWAL(cfg.DirWAL),
		zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostTags(`microstack:host`),
		zongzi.WithRaftEventListener(ctrl))
	if err != nil {
		panic(err)
	}
	ctrl.SetAgent(agent)
	start(ctx, agent, ctrl, logger)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop

	ctrl.Stop()
	agent.Stop()
}

func start(ctx context.Context, agent *zongzi.Agent, ctrl internal.ControllerCluster, logger zongzi.Logger) {
	agent.StateMachineRegister(internal.StateMachineUri, func(shardID, replicaID uint64) zongzi.StateMachine {
		return internal.NewStateMachine(logger, shardID, replicaID)
	})
	agent.StateMachineRegister(modulestore.StateMachinePersistentUri, func(shardID, replicaID uint64) zongzi.StateMachine {
		return system.NewStateMachine(logger, shardID, replicaID)
	})
	agent.StateMachineRegister(system.StateMachinePersistentUri, func(shardID, replicaID uint64) zongzi.StateMachinePersistent {
		return system.NewStateMachinePersistent(logger, shardID, replicaID)
	})
	agent.StateMachineRegister(guest.StateMachineUri, func(shardID, replicaID uint64) zongzi.StateMachine {
		return guest.NewStateMachine(logger, shardID, replicaID)
	})
	agent.StateMachineRegister(guest.StateMachinePersistentUri, func(shardID, replicaID uint64) zongzi.StateMachinePersistent {
		return guest.NewStateMachinePersistent(logger, shardID, replicaID)
	})
	ctrl.SetAgent(agent)
	if err := agent.Start(ctx); err != nil {
		panic(err)
	}
	ctrl.Start(ctx)

	// Start GRPC Server
	// Start interfaces (ie. table http api)
	// Report ready

	return
}
