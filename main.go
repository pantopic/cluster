package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/logbn/zongzi"

	"github.com/pantopic/microstack/internal"
)

func main() {
	cfg := getConfig()
	ctx := context.Background()
	agent, err := zongzi.NewAgent(cfg.ClusterName, cfg.Peers(),
		zongzi.WithRaftDir(cfg.DirRaft),
		zongzi.WithWALDir(cfg.DirWAL),
		zongzi.WithGossipAddress(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithRaftAddress(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithApiAddress(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortZongzi)))
	if err != nil {
		panic(err)
	}
	logger := getLogger(cfg)
	agent.StateMachineRegister(internal.StateMachineUri, func(shardID, replicaID uint64) zongzi.StateMachine {
		return internal.NewStateMachine(logger, shardID, replicaID)
	})
	// Load extension state machines - agent.StateMachineRegister(uri, factory)
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	// Bootstrap cluster
	// Start GRPC Server
	// Start interfaces (ie. table http api)

	// await stop
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop

	agent.Stop()
}
