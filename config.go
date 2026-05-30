package main

import (
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
		ClusterName: "pantopic",
		DirBlock:    "/var/lib/pantopic/block",
		DirLocal:    "/var/lib/pantopic/local",
		DirRaft:     "/var/lib/pantopic/raft",
		DirWAL:      "/var/lib/pantopic/wal",
		HostName:    "pantopic-a-0",
		HostPeers:   "pantopic-a-0,pantopic-b-0,pantopic-c-0",
		LogLevel:    "info",
		PortApi:     17000,
		PortZongzi:  17001,
		PortRaft:    17002,
		PortGossip:  17003,
	}
	if err := env.ParseWithOptions(&cfg, env.Options{
		Prefix: "PANTOPIC_",
	}); err != nil {
		panic(err)
	}
	return cfg
}
