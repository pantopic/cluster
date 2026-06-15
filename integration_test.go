package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/internal"
	"github.com/pantopic/cluster/pb"
)

func TestMain(t *testing.T) {
	basedir := `./tmp/pantopic`
	var ctx = context.Background()
	var mutex sync.Mutex
	var err error
	os.RemoveAll(basedir)
	peers := []string{
		`127.0.0.1:18011`,
		`127.0.0.1:18021`,
		`127.0.0.1:18031`,
	}
	agents := make([]*zongzi.Agent, len(peers))
	controllers := make([]internal.ClusterController, len(peers))
	cfg := getConfig()
	cfg.HostName = "127.0.0.1"
	logger := getLogger(cfg)
	start := func(t *testing.T, peers []string, class string, portSets ...[]uint16) {
		for i, ports := range portSets {
			controllers[i] = internal.NewClusterController(logger)
			if agents[i], err = zongzi.NewAgent(cfg.ClusterName, peers,
				zongzi.WithDirRaft(fmt.Sprintf(basedir+`/%d/raft`, i)),
				zongzi.WithDirWAL(fmt.Sprintf(basedir+`/%d/wal`, i)),
				zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[0])),
				zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[1])),
				zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[2])),
				zongzi.WithHostTags(`cluster:member`, `host:class=`+class),
				zongzi.WithRaftEventListener(controllers[i]),
			); err != nil {
				panic(err)
			}
			controllers[i].Start(ctx, agents[i])
			go run(ctx, agents[i], logger)
		}
		time.Sleep(time.Second)
		// 10 seconds to start the cluster.
		require.True(t, await(10, 100, func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			for j := range agents {
				if agents[j] == nil || agents[j].Status() != zongzi.AgentStatus_Ready {
					return false
				}
			}
			return true
		}), `%#v`, agents)
	}
	t.Run(`start`, func(t *testing.T) {
		start(t, peers, `default`,
			[]uint16{18011, 18012, 18013},
			[]uint16{18021, 18022, 18023},
			[]uint16{18031, 18032, 18033},
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
	pb.RegisterControlServer(grpcServer, internal.NewServiceControl(shardClient))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	conn, err := grpc.NewClient("localhost"+addr, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}...)
	grpcClient := pb.NewControlClient(conn)
	t.Run(`resource`, func(t *testing.T) {
		t.Run(`create`, func(t *testing.T) {
			res, err := grpcClient.ResourcePut(ctx, &pb.ResourcePutRequest{
				Item: &pb.Resource{
					Namespace: "default",
					Name:      "test",
					Uri:       "github.com/pantopic/test",
				},
			})
			require.Nil(t, err)
			require.Equal(t, uint64(0), res.Code, res.Error)
			require.Equal(t, "default", res.Item.Namespace)
			require.Equal(t, "test", res.Item.Name)
			// 10s - component is created
			// 10s - shard is created
			// 10s - shard is healthy
			// 10s - component is healthy
			// 10s - resource is healthy
		})
		t.Run(`delete`, func(t *testing.T) {
			// gRPC Api Call to delete resource
			// ensure resource status deleting
			// 10s - component is deleting
			// 10s - shard is deleting
			// 10s - shard is deleted
			// 10s - component is deleted
			// 10s - resource is deleted
		})
	})
	logger.Infof("Stopping")
	for i := range len(peers) {
		controllers[i].Stop()
	}
	for i := range len(peers) {
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
