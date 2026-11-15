package main

import (
	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"PANTOPIC_CLUSTER_NAME"`
	DirBlock    string `env:"PANTOPIC_DIR_BLOCK"`
	DirLocal    string `env:"PANTOPIC_DIR_LOCAL"`
	DirRaft     string `env:"PANTOPIC_DIR_RAFT"`
	DirWAL      string `env:"PANTOPIC_DIR_WAL"`
	HostName    string `env:"PANTOPIC_HOST_NAME"`
	HostPeers   string `env:"PANTOPIC_HOST_PEERS"`
	HostTags    string `env:"PANTOPIC_HOST_TAGS"`
	LogLevel    string `env:"PANTOPIC_LOG_LEVEL"`
	PortApi     uint16 `env:"PANTOPIC_PORT_API"`
	PortGossip  uint16 `env:"PANTOPIC_PORT_GOSSIP"`
	PortRaft    uint16 `env:"PANTOPIC_PORT_RAFT"`
	PortZongzi  uint16 `env:"PANTOPIC_PORT_ZONGZI"`
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
	if err := env.ParseWithOptions(&cfg, env.Options{}); err != nil {
		panic(err)
	}
	return cfg
}
