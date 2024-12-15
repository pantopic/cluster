package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/logbn/zongzi"
)

func main() {
	cfg := getConfig()
	ctx := context.Background()
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithRaftDir(cfg.DirRaft),
		zongzi.WithWALDir(cfg.DirWAL),
		zongzi.WithGossipAddress(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithRaftAddress(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithApiAddress(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortApi)))
	if err != nil {
		panic(err)
	}
	// Load microstack state machines
	// Load extension state machines - agent.StateMachineRegister(uri, factory)
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	// Bootstrap cluster
	// Start GRPC Server
	// Start extension servers (ie. table http api)
}
