package main

import (
	"context"
	_ "embed"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/ext-atomic/host-wazero"
	"github.com/pantopic/ext-buffer/host-wazero"
	"github.com/pantopic/ext-global/host-wazero"
	"github.com/pantopic/ext-grpc-server/host-wazero"
	"github.com/pantopic/ext-mdb/host-wazero-lmdb"
	"github.com/pantopic/ext-raft/host-wazero"
	"github.com/pantopic/ext-range-watch/host-wazero"
	"github.com/pantopic/ext-shard-client/host-wazero"
	"github.com/pantopic/ext-small-cache/host-wazero"

	"github.com/pantopic/turbokube/embed"

	"github.com/pantopic/cluster/internal"
	"github.com/pantopic/cluster/pb"
	"github.com/pantopic/cluster/test/pb"
)

//go:embed _dist/service\-grpc\.wasm
var wasmTestServiceGrpc []byte

func TestMain(t *testing.T) {
	basedir := `./tmp/pantopic`
	var ctx = context.Background()
	var err error
	os.RemoveAll(basedir)
	cfg := getConfig()
	cfg.HostName = "127.0.0.1"
	peers := []string{
		cfg.HostName + `:18011`,
		cfg.HostName + `:18021`,
		cfg.HostName + `:18031`,
	}
	agents := []*zongzi.Agent{}
	crtlClusters := []internal.ControllerCluster{}
	crtlServices := []internal.ControllerService{}
	logger := getLogger(cfg)
	var n int
	start := func(t *testing.T, peers []string, class string, portSets ...[]uint16) {
		for i, ports := range portSets {
			ctrlCluster := internal.NewControllerCluster(logger)
			agent, err := zongzi.NewAgent(cfg.ClusterName, peers,
				zongzi.WithDirRaft(fmt.Sprintf(basedir+`/%d/raft`, i+n)),
				zongzi.WithDirWAL(fmt.Sprintf(basedir+`/%d/wal`, i+n)),
				zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[0])),
				zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[1])),
				zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[2])),
				zongzi.WithHostTags(
					`cluster:member`,
					`cluster:host`,
					`host:class=`+class,
					`host:id=`+strconv.Itoa(i+n)),
				zongzi.WithRaftEventListener(ctrlCluster),
				zongzi.WithLogLevel(zongzi.LogLevelInfo))
			if err != nil {
				panic(err)
			}
			ctrlCluster.Start(ctx, agent)

			runtimeSvc := internal.NewRuntimeService(ctx, []internal.Extension{
				wazero_global.New(),
				wazero_grpc_server.New(),
				wazero_buffer.New(),
				wazero_shard_client.New(agent),
			}...)
			ctrlService := internal.NewControllerService(ctx, agent, logger, runtimeSvc)

			runtimeStorage := internal.NewRuntimeStorage(ctx, agent, []internal.Extension{
				wazero_lmdb.New(),
				wazero_global.New(),
				wazero_atomic.New(),
				wazero_range_watch.New(),
				wazero_small_cache.New(),
				wazero_raft.New(),
			}...)
			runtimeStorage.Register(agent, turbokube.StorageKvName, turbokube.StorageKvWasm)

			go run(ctx, agent, logger, ctrlService)
			ctrlService.Runtime().Register("pantopic/cluster-test/service/grpc", wasmTestServiceGrpc)
			ctrlService.Start()
			agents = append(agents, agent)
			crtlServices = append(crtlServices, ctrlService)
			crtlClusters = append(crtlClusters, ctrlCluster)
			n++
		}
		time.Sleep(time.Second)
		// 10 seconds to start the cluster.
		require.True(t, await(10, 100, func() bool {
			for j := range agents {
				if agents[j] == nil || agents[j].Status() != zongzi.AgentStatus_Ready {
					return false
				}
			}
			return true
		}), `%v`, agents[0])
	}
	t.Run(`start`, func(t *testing.T) {
		start(t, peers, `storage`,
			[]uint16{18011, 18012, 18013},
			[]uint16{18021, 18022, 18023},
			[]uint16{18031, 18032, 18033},
		)
		start(t, peers, `service`,
			[]uint16{18111, 18112, 18113},
		)
	})
	shardClient := agents[0].Client(internal.ShardID)
	t.Run(`namespaces`, func(t *testing.T) {
		for _, namespace := range []string{internal.NAMESPACE_INTERNAL, internal.NAMESPACE_DEFAULT} {
			b, err := proto.Marshal(&pb.Namespace{Name: namespace})
			q := append(b, internal.QUERY_NAMESPACE_GET)
			require.Nil(t, err)
			var val uint64
			var data []byte
			// 10 seconds for namespaces to be created
			require.True(t, await(10, 100, func() bool {
				val, data, err = shardClient.Read(ctx, q, true)
				return err == nil && val == internal.ResultSuccess && len(data) > 0
			}), `%d %s %s %v`, val, data, namespace, err)
		}
	})
	t.Run(`cluster`, func(t *testing.T) {
		req := &pb.Cluster{Name: cfg.ClusterName}
		reqBytes, err := proto.Marshal(req)
		require.Nil(t, err)
		q := append(reqBytes, internal.QUERY_CLUSTER_GET)
		var val uint64
		var data []byte
		// 10 seconds for namespaces to be created
		require.True(t, await(10, 100, func() bool {
			val, data, err = shardClient.Read(ctx, q, true)
			return err == nil && val == internal.ResultSuccess && len(data) > 0
		}), `%d %s %s %v`, val, data, cfg.ClusterName, err)
	})
	// gRPC server
	var grpcServer = grpc.NewServer([]grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Hour,
			Timeout: 20 * time.Second,
		}),
	}...)
	addr := fmt.Sprintf(":%d", cfg.PortApi)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	pb.RegisterControlServer(grpcServer, internal.NewGrpcServiceControl(shardClient))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	// Register test model

	conn, err := grpc.NewClient("localhost"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	ctrlClient := pb.NewControlClient(conn)
	resource := &pb.Resource{
		Namespace: "default",
		Name:      "test",
		Type:      "pantopic/cluster-test",
	}
	service := &pb.Service{
		Namespace: "default",
		Resource:  resource.Name,
		Name:      "test",
		Type:      "grpc",
		Port:      19000,
		Status:    internal.Status_Active,
		Tags: map[string]string{
			"placement:cover": "host:class=service",
		},
	}
	component := &pb.Component{
		Namespace: "default",
		Resource:  resource.Name,
		Cluster:   cfg.ClusterName,
		Name:      "test",
		Type:      "component",
		Tags: map[string]string{
			"placement:cover": "host:class=storage",
			"placement:vary":  "geo:zone",
		},
	}
	t.Run(`resource`, func(t *testing.T) {
		t.Run(`create`, func(t *testing.T) {
			putResp, err := ctrlClient.ResourcePut(ctx, &pb.ResourcePutRequest{
				Item: resource,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, resource.Namespace, putResp.Item.Namespace)
			require.Equal(t, resource.Name, putResp.Item.Name)
			require.Equal(t, resource.Type, putResp.Item.Type)
		})
	})
	t.Run(`service`, func(t *testing.T) {
		t.Run(`create`, func(t *testing.T) {
			putResp, err := ctrlClient.ServicePut(ctx, &pb.ServicePutRequest{
				Item: service,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, service.Namespace, putResp.Item.Namespace)
			require.Equal(t, service.Name, putResp.Item.Name)
			require.Equal(t, service.Type, putResp.Item.Type)
		})
		t.Run(`list`, func(t *testing.T) {
			// 10s - service is created and healthy
			svcListReq := &pb.ServiceListRequest{
				Namespace: resource.Namespace,
				Resource:  resource.Name,
			}
			var svcListResp *pb.ServiceListResponse
			require.True(t, await(10, 100, func() bool {
				svcListResp, err = ctrlClient.ServiceList(ctx, svcListReq)
				return err == nil &&
					svcListResp.Code == internal.ResultSuccess &&
					len(svcListResp.Item) > 0 &&
					svcListResp.Item[0].Status == internal.Status_Active
			}), "%#v, %#v", svcListResp, err)
			require.Equal(t, 1, len(svcListResp.Item))
			require.Equal(t, `grpc`, svcListResp.Item[0].Type)
		})
		t.Run(`query`, func(t *testing.T) {
			conn, err := grpc.NewClient("localhost:19000", grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				panic(err)
			}
			svcClient := testpb.NewTestClient(conn)
			var res *testpb.PingResponse
			require.True(t, await(10, 100, func() bool {
				res, err = svcClient.Ping(ctx, &testpb.PingRequest{})
				return err == nil
			}), "%s", err)
			require.Nil(t, err, err)
			require.NotNil(t, res)
		})
	})
	t.Run(`component`, func(t *testing.T) {
		t.Run(`create`, func(t *testing.T) {
			putResp, err := ctrlClient.ComponentPut(ctx, &pb.ComponentPutRequest{
				Item: component,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, component.Namespace, putResp.Item.Namespace)
			require.Equal(t, component.Name, putResp.Item.Name)
			require.Equal(t, component.Resource, putResp.Item.Resource)
			require.Equal(t, component.Type, putResp.Item.Type)
		})
		t.Run(`list`, func(t *testing.T) {
			// 10s - component is created and healthy
			compListReq := &pb.ComponentListRequest{
				Namespace: resource.Namespace,
				Resource:  resource.Name,
			}
			var compListResp *pb.ComponentListResponse
			require.True(t, await(10, 100, func() bool {
				compListResp, err = ctrlClient.ComponentList(ctx, compListReq)
				return err == nil && compListResp.Code == internal.ResultSuccess && len(compListResp.Item) > 0
			}), err)
			require.Equal(t, 1, len(compListResp.Item))
			require.Equal(t, `component`, compListResp.Item[0].Type)
		})
	})
	// service set/get
	t.Run(`delete`, func(t *testing.T) {
		// gRPC Api Call to delete resource
		// ensure resource status deleting
		// 10s - component is deleting
		// 10s - shard is deleting
		// 10s - shard is deleted
		// 10s - component is deleted
		// 10s - resource is deleted
	})
	logger.Infof("Stopping")
	for i := range len(crtlClusters) {
		logger.Infof("Stopping cluster %d", i)
		crtlClusters[i].Stop()
	}
	for i := range len(agents) {
		logger.Infof("Stopping agent %d", i)
		agents[i].Stop()
	}
}

func await(d, n time.Duration, fn func() bool) bool {
	for range n {
		if fn() {
			return true
		}
		time.Sleep(d * time.Second / n)
	}
	return false
}
