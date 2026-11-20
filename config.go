package cluster

import (
	"strings"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	ClusterName string `env:"CLUSTER_NAME"`
	Dir         string `env:"DIR"`
	DirBlock    string `env:"DIR_BLOCK"`
	DirLocal    string `env:"DIR_LOCAL"`
	DirRaft     string `env:"DIR_RAFT"`
	DirWAL      string `env:"DIR_WAL"`
	HostName    string `env:"HOST_NAME"`
	HostPeers   string `env:"HOST_PEERS"`
	HostTags    string `env:"HOST_TAGS"`
	LogLevel    string `env:"LOG_LEVEL"`
	PortApi     uint16 `env:"PORT_API"`
	PortGossip  uint16 `env:"PORT_GOSSIP"`
	PortRaft    uint16 `env:"PORT_RAFT"`
	PortZongzi  uint16 `env:"PORT_ZONGZI"`
}

var DefaultConfig = Config{
	ClusterName: "pantopic",
	Dir:         "/var/lib/pantopic",
	HostName:    "pantopic-a-0",
	HostPeers:   "pantopic-a-0,pantopic-b-0,pantopic-c-0",
	HostTags:    strings.Join([]string{`cluster:member`, `cluster:host`}, "|"),
	LogLevel:    "info",
	PortApi:     17000,
	PortGossip:  17003,
	PortRaft:    17002,
	PortZongzi:  17001,
}

func EnvConfig(opts ...ConfigOption) Config {
	o := env.Options{Prefix: "PANTOPIC_"}
	cfg := DefaultConfig
	for _, opt := range opts {
		cfg = opt(cfg, &o)
	}
	if err := env.ParseWithOptions(&cfg, o); err != nil {
		panic(err)
	}
	if cfg.DirBlock == "" && cfg.Dir != "" {
		cfg.DirBlock = cfg.Dir + "/block"
	}
	if cfg.DirLocal == "" && cfg.Dir != "" {
		cfg.DirLocal = cfg.Dir + "/local"
	}
	if cfg.DirRaft == "" && cfg.Dir != "" {
		cfg.DirRaft = cfg.Dir + "/raft"
	}
	if cfg.DirWAL == "" && cfg.Dir != "" {
		cfg.DirWAL = cfg.Dir + "/wal"
	}
	return cfg
}

type ConfigOption func(Config, *env.Options) Config

func WithEnvPrefix(prefix string) ConfigOption {
	return func(cfg Config, o *env.Options) Config {
		o.Prefix = prefix
		return cfg
	}
}

func WithDefault(newDefault Config) ConfigOption {
	return func(cfg Config, o *env.Options) Config {
		return newDefault
	}
}
