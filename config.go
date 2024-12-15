package main

import (
	"github.com/caarlos0/env/v11"
)

type config struct {
	ClusterName string `env:"MICROSTACK_CLUSTER_NAME"`
	DirData     string `env:"MICROSTACK_DIR_DATA"`
	DirRaft     string `env:"MICROSTACK_DIR_RAFT"`
	DirWAL      string `env:"MICROSTACK_DIR_WAL"`
	HostName    string `env:"MICROSTACK_HOST_NAME"`
	HostPeers   string `env:"MICROSTACK_HOST_PEERS"`
	ListenAddr  string `env:"MICROSTACK_LISTEN_ADDR"`
	PortApi     uint16 `env:"MICROSTACK_PORT_API"`
	PortGossip  uint16 `env:"MICROSTACK_PORT_GOSSIP"`
	PortRaft    uint16 `env:"MICROSTACK_PORT_RAFT"`
	PortZongzi  uint16 `env:"MICROSTACK_PORT_ZONGZI"`
}

func getConfig() config {
	cfg := config{
		ClusterName: "microstack-1",
		DirData:     "/var/lib/microstack/data",
		DirRaft:     "/var/lib/microstack/raft",
		DirWAL:      "/var/lib/microstack/wal",
		HostName:    "microstack-1-a-0",
		HostPeers:   "microstack-1-a-0,microstack-1-b-0,microstack-1-c-0",
		ListenAddr:  "",
		PortApi:     8000,
		PortGossip:  17001,
		PortRaft:    17002,
		PortZongzi:  17003,
	}
	if err := env.Parse(&cfg); err != nil {
		panic(err)
	}
	return cfg
}
