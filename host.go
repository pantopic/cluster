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
	ctrlBank       internal.ControllerBank
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

func New(ctx context.Context, cfg Config) *Host {
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

		ctrlBank    = internal.NewControllerBank(logger, agent, runtimeControl)
		ctrlCluster = internal.NewControllerCluster(logger, agent)
		ctrlBridge  = internal.NewControllerBridge(logger, agent, runtimeBridge)
		ctrlService = internal.NewControllerService(logger, agent, runtimeService)

		fsm = internal.StateMachineFactory(logger, ctrlService, ctrlBank)
	)
	agent.StateMachineRegister(internal.StateMachineUri, fsm)
	return &Host{
		agent:          agent,
		cfg:            cfg,
		ctrlBank:       ctrlBank,
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
	h.agent.AddRaftEventListener(h.ctrlCluster)
	h.ctrlCluster.Start(h.ctx)
	h.ctrlBridge.Start(h.ctx)
	h.wg.Go(func() {
		if err := h.agent.Start(h.ctx); err != nil {
			panic(err)
		}
	})
	h.ctrlService.Start(h.ctx)
	h.ctrlBank.Start(h.ctx)
	h.serverControl.Start()
	return nil
}

func (h *Host) Stop() error {
	h.ctrlBridge.Stop()
	h.ctrlService.Stop()
	h.ctrlBank.Stop()
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
