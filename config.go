package main

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"CLUSTER_NAME"`
	DirBlock    string `env:"DIR_BLOCK"`
	DirLocal    string `env:"DIR_LOCAL"`
	DirRaft     string `env:"DIR_RAFT"`
	DirWAL      string `env:"DIR_WAL"`
	HostName    string `env:"HOST_NAME"`
	HostPeers   string `env:"HOST_PEERS"`
	LogLevel    string `env:"LOG_LEVEL"`
	PortApi     uint16 `env:"PORT_API"`
	PortGossip  uint16 `env:"PORT_GOSSIP"`
	PortRaft    uint16 `env:"PORT_RAFT"`
	PortZongzi  uint16 `env:"PORT_ZONGZI"`
}

func getConfig() config {
	cfg := config{
		ClusterName: "microstack",
		DirBlock:    "/var/lib/microstack/block",
		DirLocal:    "/var/lib/microstack/local",
		DirRaft:     "/var/lib/microstack/raft",
		DirWAL:      "/var/lib/microstack/wal",
		HostName:    "microstack-a-0",
		HostPeers:   "microstack-a-0,microstack-b-0,microstack-c-0",
		LogLevel:    "warn",
		PortApi:     8000,
		PortGossip:  17001,
		PortRaft:    17002,
		PortZongzi:  17003,
	}
	if err := env.ParseWithOptions(&cfg, env.Options{
		Prefix: "MICROSTACK_",
	}); err != nil {
		panic(err)
	}
	return cfg
}

func (cfg config) Peers() (peers []string) {
	peers = strings.Split(cfg.HostPeers, ",")
	for i, addr := range peers {
		if !strings.Contains(addr, ":") {
			peers[i] = fmt.Sprintf("%s:%d", addr, cfg.PortZongzi)
		}
	}
	return
}
