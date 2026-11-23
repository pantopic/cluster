package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/logbn/zongzi"

	"github.com/pantopic/ext-atomic/host-wazero"
	"github.com/pantopic/ext-buffer/host-wazero"
	"github.com/pantopic/ext-cluster/host-wazero"
	"github.com/pantopic/ext-cron/host-wazero"
	"github.com/pantopic/ext-global/host-wazero"
	"github.com/pantopic/ext-grpc-server/host-wazero"
	"github.com/pantopic/ext-http-server/host-wazero"
	"github.com/pantopic/ext-mdb/host-wazero-lmdb"
	"github.com/pantopic/ext-range-watch/host-wazero"
	"github.com/pantopic/ext-shard-client/host-wazero"
	"github.com/pantopic/ext-small-cache/host-wazero"

	"github.com/pantopic/cluster/internal"
	"github.com/pantopic/cluster/pb"
)

type Host struct {
	agent          *zongzi.Agent
	cfg            Config
	ctrlComponent  internal.ControllerComponent
	ctrlCluster    internal.ControllerCluster
	ctrlService    internal.ControllerService
	ctrlBridge     internal.ControllerBridge
	ctx            context.Context
	extGlobal      wazero_global.HostModule
	logger         Logger
	runtimeControl internal.RuntimeControl
	runtimeBridge  internal.RuntimeBridge
	runtimeService internal.RuntimeService
	runtimeStorage internal.RuntimeStorage
	serverControl  *serverControl
	wg             sync.WaitGroup
}

func New(ctx context.Context, cfg Config, opts ...HostOption) *Host {
	agent, err := zongzi.NewAgent(cfg.ClusterName, strings.Split(cfg.HostPeers, ","),
		zongzi.WithDirRaft(cfg.DirRaft),
		zongzi.WithDirWAL(cfg.DirWAL),
		zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortGossip)),
		zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortRaft)),
		zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortZongzi)),
		zongzi.WithHostTags(strings.Split(cfg.HostTags, "|")...))
	if err != nil {
		panic(err)
	}
	var (
		apiAddr        = fmt.Sprintf(`%s:%d`, cfg.HostName, cfg.PortApi)
		logger         = getLogger(cfg)
		extGlobal      = wazero_global.New()
		extShardClient = wazero_shard_client.New(agent)

		runtimeControl = internal.NewRuntimeControl(ctx, agent, []internal.Extension{
			extGlobal,
			extShardClient,
		}...)
		runtimeBridge = internal.NewRuntimeBridge(ctx, agent, []internal.Extension{
			extGlobal,
			extShardClient,
			wazero_atomic.New(),
			wazero_cron.New(),
		}...)
		runtimeService = internal.NewRuntimeService(ctx, []internal.Extension{
			extGlobal,
			extShardClient,
			wazero_buffer.New(),
			wazero_cluster.New(agent),
			wazero_grpc_server.New(),
			wazero_http_server.New(),
		}...)
		runtimeStorage = internal.NewRuntimeStorage(ctx, agent, []internal.Extension{
			extGlobal,
			wazero_atomic.New(),
			wazero_lmdb.New(wazero_lmdb.WithDirLocal(cfg.DirLocal)),
			wazero_range_watch.New(),
			wazero_small_cache.New(),
		}...)

		ctrlComponent = internal.NewControllerComponent(logger, agent, runtimeControl)
		ctrlCluster   = internal.NewControllerCluster(logger, agent)
		ctrlBridge    = internal.NewControllerBridge(logger, agent, runtimeBridge)
		ctrlService   = internal.NewControllerService(logger, agent, runtimeService)
	)
	agent.StateMachineRegister(internal.StateMachineUri,
		internal.StateMachineFactory(logger, ctrlService, ctrlComponent),
	)
	h := &Host{
		agent:          agent,
		cfg:            cfg,
		ctrlComponent:  ctrlComponent,
		ctrlCluster:    ctrlCluster,
		ctrlBridge:     ctrlBridge,
		ctrlService:    ctrlService,
		ctx:            ctx,
		extGlobal:      extGlobal,
		logger:         logger,
		runtimeControl: runtimeControl,
		runtimeBridge:  runtimeBridge,
		runtimeService: runtimeService,
		runtimeStorage: runtimeStorage,
		serverControl:  newServerControl(agent, apiAddr),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Host) Agent() *zongzi.Agent {
	return h.agent
}

func (h *Host) ControlClient() pb.ControlClient {
	return h.serverControl.Client()
}

func (h *Host) GlobalSet(k string, v uint64) {
	h.extGlobal.Set(k, v)
}

func (h *Host) GlobalDel(k string) {
	h.extGlobal.Del(k)
}

func (h *Host) Start() error {
	h.ctrlCluster.Start(h.ctx)
	if err := h.agent.Start(h.ctx); err != nil {
		panic(err)
	}
	h.ctrlBridge.Start(h.ctx)
	h.ctrlService.Start(h.ctx)
	h.ctrlComponent.Start(h.ctx)
	h.serverControl.Start()
	h.logger.Infof(`%s Host Started`, h.agent.HostID())
	return nil
}

func (h *Host) Stop() error {
	defer h.logger.Infof(`%s Host Stopped`, h.agent.HostID())
	h.serverControl.Stop()
	h.ctrlBridge.Stop()
	h.ctrlService.Stop()
	h.ctrlComponent.Stop()
	h.ctrlCluster.Stop()
	h.agent.Stop()
	return nil
}

func (h *Host) RuntimeControl() internal.RuntimeControl {
	return h.runtimeControl
}

func (h *Host) RuntimeService() internal.RuntimeService {
	return h.runtimeService
}

func (h *Host) RuntimeStorage() internal.RuntimeStorage {
	return h.runtimeStorage
}

func (h *Host) RuntimeBridge() internal.RuntimeBridge {
	return h.runtimeBridge
}

func (h *Host) RegisterControl(name string, wasm []byte) {
	h.runtimeControl.Register(name, wasm)
}

func (h *Host) RegisterService(name string, wasm []byte) {
	h.runtimeService.Register(name, wasm)
}

func (h *Host) RegisterStorage(name string, wasm []byte) {
	h.runtimeStorage.Register(name, wasm)
}

func (h *Host) RegisterBridge(name string, wasm []byte) {
	h.runtimeBridge.Register(name, wasm)
}
