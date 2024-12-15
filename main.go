package main

import (
	"context"
	"flag"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/logbn/zongzi"
)

type config struct {
	AddrGossip   string `env:"MICROSTACK_ADDR_GOSSIP" envDefault:"10.0.0.1:17001"`
	AddrGrpcExt  string `env:"MICROSTACK_ADDR_GRPC_EXT" envDefault:"10.0.0.1:17004"`
	AddrGrpcInt  string `env:"MICROSTACK_ADDR_GRPC_INT" envDefault:"10.0.0.1:17003"`
	AddrRaft     string `env:"MICROSTACK_ADDR_RAFT" envDefault:"10.0.0.1:17002"`
	ClusterName  string `env:"MICROSTACK_CLUSTER_NAME" envDefault:"microstack"`
	ClusterPeers string `env:"MICROSTACK_CLUSTER_PEERS" envDefault:"microstack-a-0,microstack-b-0,microstack-c-0"`
	DirData      string `env:"MICROSTACK_DIR_DATA" envDefault:"/var/lib/microstack/data"`
	DirRaft      string `env:"MICROSTACK_DIR_RAFT" envDefault:"/var/lib/microstack/raft"`
	DirWAL       string `env:"MICROSTACK_DIR_WAL" envDefault:"/var/lib/microstack/wal"`
}

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		panic(err)
	}

	flag.Parse()
	ctx := context.Background()
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.ClusterPeers, ","),
		zongzi.WithRaftDir(cfg.DirRaft),
		zongzi.WithWALDir(cfg.DirWAL),
		zongzi.WithGossipAddress(cfg.AddrGossip),
		zongzi.WithRaftAddress(cfg.AddrRaft),
		zongzi.WithApiAddress(cfg.AddrGrpcInt))
	if err != nil {
		panic(err)
	}
	// Load extensions
	// agent.StateMachineRegister(uri, factory)
	if err = agent.Start(ctx); err != nil {
		panic(err)
	}
	// Start GRPC Server
}
