package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/logbn/zongzi"
	"github.com/pantopic/cluster/internal"

	"github.com/pantopic/ext-atomic/host-wazero"
	"github.com/pantopic/ext-buffer/host-wazero"
	"github.com/pantopic/ext-global/host-wazero"
	"github.com/pantopic/ext-grpc-server/host-wazero"
	"github.com/pantopic/ext-mdb/host-wazero-lmdb"
	"github.com/pantopic/ext-range-watch/host-wazero"
	"github.com/pantopic/ext-shard-client/host-wazero"
	"github.com/pantopic/ext-small-cache/host-wazero"
)

type Cluster interface {
	Start() error
	Stop() error
}

type cluster struct {
	agent          *zongzi.Agent
	cfg            config
	ctrlCluster    internal.ControllerCluster
	ctrlManager    internal.ControllerManager
	ctrlService    internal.ControllerService
	ctrlRelay      internal.ControllerRelay
	logger         Logger
	runtimeControl internal.RuntimeControl
	runtimeRelay   internal.RuntimeRelay
	runtimeService internal.RuntimeService
	runtimeStorage internal.RuntimeStorage
}

func newCluster(ctx context.Context, cfg config, logger Logger) *cluster {
	opts := []zongzi.AgentOption{
		zongzi.WithDirRaft(cfg.DirRaft),
		zongzi.WithDirWAL(cfg.DirWAL),
		zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortZongzi)),
	}
	if cfg.HostTags != "" {
		opts = append(opts, zongzi.WithHostTags(strings.Split(cfg.HostTags, "|")...))
	}
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","), opts...)
	if err != nil {
		panic(err)
	}
	runtimeService := internal.NewRuntimeService(ctx, []internal.Extension{
		wazero_global.New(),
		wazero_grpc_server.New(),
		wazero_buffer.New(),
		wazero_shard_client.New(agent),
	}...)
	runtimeRelay := internal.NewRuntimeRelay(ctx, agent, []internal.Extension{
		wazero_shard_client.New(agent),
	}...)
	runtimeStorage := internal.NewRuntimeStorage(ctx, agent, []internal.Extension{
		wazero_lmdb.New(),
		wazero_global.New(),
		wazero_atomic.New(),
		wazero_range_watch.New(),
		wazero_small_cache.New(),
	}...)
	runtimeControl := internal.NewRuntimeControl(ctx, agent, []internal.Extension{
		wazero_shard_client.New(agent),
	}...)
	return &cluster{
		agent:          agent,
		cfg:            cfg,
		ctrlCluster:    internal.NewControllerCluster(logger, agent),
		ctrlManager:    internal.NewControllerManager(logger, agent, runtimeControl),
		ctrlRelay:      internal.NewControllerRelay(logger, agent, runtimeRelay),
		ctrlService:    internal.NewControllerService(logger, agent, runtimeService),
		logger:         logger,
		runtimeControl: runtimeControl,
		runtimeRelay:   runtimeRelay,
		runtimeService: runtimeService,
		runtimeStorage: runtimeStorage,
	}
}

func (c *cluster) Start(ctx context.Context) error {
	c.agent.AddRaftEventListener(c.ctrlCluster)
	c.ctrlCluster.Start(ctx)
	c.ctrlRelay.Start(ctx)
	if err := c.agent.Start(ctx); err != nil {
		panic(err)
	}
	c.ctrlService.Start(ctx)
	c.ctrlManager.Start(ctx)
	return nil
}

func (c *cluster) Stop() error {
	c.ctrlRelay.Stop()
	c.ctrlService.Stop()
	c.ctrlManager.Stop()
	c.ctrlCluster.Stop()
	c.agent.Stop()
	return nil
}

func (c *cluster) RuntimeControl() internal.RuntimeControl {
	return c.runtimeControl
}

func (c *cluster) RuntimeService() internal.RuntimeService {
	return c.runtimeService
}

func (c *cluster) RuntimeStorage() internal.RuntimeStorage {
	return c.runtimeStorage
}

func (c *cluster) RuntimeRelay() internal.RuntimeRelay {
	return c.runtimeRelay
}
