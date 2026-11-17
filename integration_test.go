package cluster

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/kitsune/embed"
	"github.com/pantopic/kitsune/test"
	"github.com/pantopic/turbokube/embed"

	"github.com/pantopic/cluster/internal"
	"github.com/pantopic/cluster/pb"
	"github.com/pantopic/cluster/test/pb"
)

//go:embed _dist/service-grpc.dev.wasm
var wasmTestServiceGrpc []byte

//go:embed _dist/storage.dev.wasm
var wasmTestStorage []byte

//go:embed _dist/control.dev.wasm
var wasmTestControl []byte

func TestHost(t *testing.T) {
	var cfg = EnvConfig()
	cfg.HostName = "127.0.0.1"
	cfg.LogLevel = "debug"
	var (
		basedir = `./tmp/pantopic`
		hosts   = []*Host{}
		ctx     = context.Background()
		err     error
		peers   = []string{
			cfg.HostName + `:18011`,
			cfg.HostName + `:18021`,
			cfg.HostName + `:18031`,
		}
		n = 0
	)
	os.RemoveAll(basedir)
	start := func(t *testing.T, peers []string, class string, portSets ...[]uint16) {
		for i, ports := range portSets {
			cfg := cfg
			cfg.DirBlock = fmt.Sprintf(basedir+"/%d/block", n)
			cfg.DirLocal = fmt.Sprintf(basedir+"/%d/local", n)
			cfg.DirRaft = fmt.Sprintf(basedir+`/%d/raft`, n)
			cfg.DirWAL = fmt.Sprintf(basedir+`/%d/wal`, n)
			cfg.PortZongzi = ports[0]
			cfg.PortRaft = ports[1]
			cfg.PortGossip = ports[2]
			cfg.PortApi = ports[3]
			cfg.HostTags = strings.Join([]string{
				`cluster:member`,
				`cluster:host`,
				`host:class=` + class,
				`geo:zone=test-` + strconv.Itoa(i%3),
				`host:id=` + strconv.Itoa(n),
			}, "|")
			cfg.HostPeers = strings.Join(peers, ",")
			host := New(ctx, cfg)

			// Test
			host.RegisterService("pantopic/cluster-test/service/grpc", wasmTestServiceGrpc)
			host.RegisterStorage("pantopic/cluster-test/storage/test", wasmTestStorage)
			host.RegisterControl("pantopic/cluster-test/control/test", wasmTestControl)

			turbokube.Register(host)
			kitsune.Register(host)

			if err := host.Start(); err != nil {
				panic(err)
			}
			hosts = append(hosts, host)
			n++
		}
		time.Sleep(time.Second)
		// 10 seconds to start the cluster.
		require.True(t, await(10, 100, func() bool {
			for j := range hosts {
				if hosts[j] == nil || hosts[j].agent.Status() != zongzi.AgentStatus_Ready {
					return false
				}
			}
			return true
		}), `%v`, hosts[0])
	}
	t.Run(`start`, func(t *testing.T) {
		start(t, peers, `storage`,
			[]uint16{18011, 18012, 18013, 19010},
			[]uint16{18021, 18022, 18023, 19020},
			[]uint16{18031, 18032, 18033, 19030},
		)
		start(t, peers, `service`,
			[]uint16{18111, 18112, 18113, 19040},
		)
	})
	host := hosts[0]
	shardClient := host.agent.Client(internal.ShardID)
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

	t.Run(`cluster-test`, func(t *testing.T) {
		port := uint64(19000)
		addr := fmt.Sprintf("localhost:%d", port)
		resource := &pb.Resource{
			Namespace: "default",
			Name:      "cluster-test",
			Type:      "pantopic/cluster-test",
		}
		service := &pb.Service{
			Namespace: "default",
			Resource:  resource.Name,
			Name:      "test",
			Type:      "grpc",
			Port:      port,
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
			Type:      "test",
			Tags: map[string]string{
				"placement:member": "3;host:class=storage",
				"placement:vary":   "geo:zone",
			},
		}
		t.Run(`provision`, func(t *testing.T) {
			t.Run(`resource`, func(t *testing.T) {
				putResp, err := ctrlClient.ResourcePut(ctx, &pb.ResourcePutRequest{
					Item: resource,
				})
				require.Nil(t, err, err)
				require.Equal(t, uint64(0), putResp.Code, putResp.Error)
				require.Equal(t, resource.Namespace, putResp.Item.Namespace)
				require.Equal(t, resource.Name, putResp.Item.Name)
				require.Equal(t, resource.Type, putResp.Item.Type)
			})
			t.Run(`service`, func(t *testing.T) {
				putResp, err := ctrlClient.ServicePut(ctx, &pb.ServicePutRequest{
					Item: service,
				})
				require.Nil(t, err, err)
				require.Equal(t, uint64(0), putResp.Code, putResp.Error)
				require.Equal(t, service.Namespace, putResp.Item.Namespace)
				require.Equal(t, service.Name, putResp.Item.Name)
				require.Equal(t, service.Type, putResp.Item.Type)
			})
			t.Run(`component`, func(t *testing.T) {
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
			t.Run(`active`, func(t *testing.T) {
				t.Run(`service`, func(t *testing.T) {
					svcListReq := &pb.ServiceListRequest{
						Namespace: resource.Namespace,
						Resource:  resource.Name,
					}
					var svcListResp *pb.ServiceListResponse
					// 10s - service is created and active
					require.True(t, await(10, 100, func() bool {
						svcListResp, err = ctrlClient.ServiceList(ctx, svcListReq)
						return err == nil &&
							svcListResp.Code == internal.ResultSuccess &&
							len(svcListResp.Item) > 0 &&
							svcListResp.Item[0].Status == internal.Status_Active
					}), "%#v, %#v", svcListResp, err)
					require.Equal(t, 1, len(svcListResp.Item))
					require.Equal(t, service.Type, svcListResp.Item[0].Type)
				})
				t.Run(`component`, func(t *testing.T) {
					compListReq := &pb.ComponentListRequest{
						Namespace: resource.Namespace,
						Resource:  resource.Name,
					}
					var compListResp *pb.ComponentListResponse
					// 10s - component is created and active
					require.True(t, await(10, 100, func() bool {
						compListResp, err = ctrlClient.ComponentList(ctx, compListReq)
						return err == nil &&
							compListResp.Code == internal.ResultSuccess &&
							len(compListResp.Item) > 0 &&
							compListResp.Item[0].Status == internal.Status_Active
					}), err)
					require.Equal(t, 1, len(compListResp.Item))
					require.Equal(t, component.Type, compListResp.Item[0].Type)
				})
			})
		})
		t.Run(`query`, func(t *testing.T) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				panic(err)
			}
			svcClient := testpb.NewTestClient(conn)
			var res *testpb.PingResponse
			assert.True(t, await(5, 10, func() bool {
				res, err = svcClient.Ping(ctx, &testpb.PingRequest{})
				return err == nil
			}), "%s", err)
			require.Nil(t, err, err)
			require.NotNil(t, res)
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
	t.Run(`kitsune`, func(t *testing.T) {
		port := 19100
		addr := fmt.Sprintf("localhost:%d", port)
		t.Run(`provision`, func(t *testing.T) {
			resource := &pb.Resource{
				Namespace: "default",
				Name:      "kitsune-test",
				Type:      "pantopic/kitsune",
			}
			service := &pb.Service{
				Namespace: "default",
				Resource:  resource.Name,
				Name:      "api",
				Type:      "http",
				Port:      uint64(port),
				Status:    internal.Status_Active,
				Tags: map[string]string{
					"placement:cover": "host:class=service",
				},
			}
			component := &pb.Component{
				Namespace: "default",
				Resource:  resource.Name,
				Cluster:   cfg.ClusterName,
				Name:      "default",
				Type:      "basic",
				Tags: map[string]string{
					"placement:member": "3;host:class=storage",
					"placement:vary":   "geo:zone",
				},
			}
			t.Run(`resource`, func(t *testing.T) {
				putResp, err := ctrlClient.ResourcePut(ctx, &pb.ResourcePutRequest{
					Item: resource,
				})
				require.Nil(t, err, err)
				require.Equal(t, uint64(0), putResp.Code, putResp.Error)
				require.Equal(t, resource.Namespace, putResp.Item.Namespace)
				require.Equal(t, resource.Name, putResp.Item.Name)
				require.Equal(t, resource.Type, putResp.Item.Type)
			})
			t.Run(`service`, func(t *testing.T) {
				putResp, err := ctrlClient.ServicePut(ctx, &pb.ServicePutRequest{
					Item: service,
				})
				require.Nil(t, err, err)
				require.Equal(t, uint64(0), putResp.Code, putResp.Error)
				require.Equal(t, service.Namespace, putResp.Item.Namespace)
				require.Equal(t, service.Name, putResp.Item.Name)
				require.Equal(t, service.Type, putResp.Item.Type)
			})
			t.Run(`component`, func(t *testing.T) {
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
			t.Run(`active`, func(t *testing.T) {
				t.Run(`service`, func(t *testing.T) {
					svcListReq := &pb.ServiceListRequest{
						Namespace: resource.Namespace,
						Resource:  resource.Name,
					}
					var svcListResp *pb.ServiceListResponse
					// 10s - service is created and active
					require.True(t, await(10, 100, func() bool {
						svcListResp, err = ctrlClient.ServiceList(ctx, svcListReq)
						return err == nil &&
							svcListResp.Code == internal.ResultSuccess &&
							len(svcListResp.Item) > 0 &&
							svcListResp.Item[0].Status == internal.Status_Active
					}), "%#v, %#v", svcListResp, err)
					require.Equal(t, 1, len(svcListResp.Item))
					require.Equal(t, service.Type, svcListResp.Item[0].Type)
				})
				t.Run(`component`, func(t *testing.T) {
					compListReq := &pb.ComponentListRequest{
						Namespace: resource.Namespace,
						Resource:  resource.Name,
					}
					var compListResp *pb.ComponentListResponse
					// 10s - component is created and active
					require.True(t, await(10, 100, func() bool {
						compListResp, err = ctrlClient.ComponentList(ctx, compListReq)
						return err == nil &&
							compListResp.Code == internal.ResultSuccess &&
							len(compListResp.Item) > 0 &&
							compListResp.Item[0].Status == internal.Status_Active
					}), err)
					require.Equal(t, 1, len(compListResp.Item))
					require.Equal(t, component.Type, compListResp.Item[0].Type)
				})
			})
		})
		var c = &http.Client{}
		var res *http.Response
		t.Run(`healthcheck`, func(t *testing.T) {
			assert.True(t, await(5, 10, func() bool {
				res, err = c.Get(fmt.Sprintf("http://%s/health", addr))
				return err == nil && res.StatusCode == 200
			}), "timeout")
			require.Nil(t, err, err)
			require.NotNil(t, res)
			body, err := io.ReadAll(res.Body)
			require.Equal(t, http.StatusOK, res.StatusCode, "%s", string(body))
			require.Nil(t, err, err)
			require.Equal(t, "Okay", string(body))
			defer res.Body.Close()
		})
		kitsune_test.All(t, c, addr)
	})
	t.Run(`turbokube`, func(t *testing.T) {
		port := 19200
		addr := fmt.Sprintf("localhost:%d", port)
		t.Run(`provision`, func(t *testing.T) {
			resource := &pb.Resource{
				Namespace: "default",
				Name:      "turbokube-test",
				Type:      "pantopic/turbokube",
			}
			service := &pb.Service{
				Namespace: "default",
				Resource:  resource.Name,
				Name:      "api",
				Type:      "grpc",
				Port:      uint64(port),
				Status:    internal.Status_Active,
				Tags: map[string]string{
					"placement:cover": "host:class=service",
				},
			}
			component := &pb.Component{
				Namespace: "default",
				Resource:  resource.Name,
				Cluster:   cfg.ClusterName,
				Name:      "default",
				Type:      "basic",
				Tags: map[string]string{
					"placement:member": "3;host:class=storage",
					"placement:vary":   "geo:zone",
				},
			}
			t.Run(`resource`, func(t *testing.T) {
				putResp, err := ctrlClient.ResourcePut(ctx, &pb.ResourcePutRequest{
					Item: resource,
				})
				require.Nil(t, err, err)
				require.Equal(t, uint64(0), putResp.Code, putResp.Error)
				require.Equal(t, resource.Namespace, putResp.Item.Namespace)
				require.Equal(t, resource.Name, putResp.Item.Name)
				require.Equal(t, resource.Type, putResp.Item.Type)
			})
			t.Run(`service`, func(t *testing.T) {
				putResp, err := ctrlClient.ServicePut(ctx, &pb.ServicePutRequest{
					Item: service,
				})
				require.Nil(t, err, err)
				require.Equal(t, uint64(0), putResp.Code, putResp.Error)
				require.Equal(t, service.Namespace, putResp.Item.Namespace)
				require.Equal(t, service.Name, putResp.Item.Name)
				require.Equal(t, service.Type, putResp.Item.Type)
			})
			t.Run(`component`, func(t *testing.T) {
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
			t.Run(`active`, func(t *testing.T) {
				t.Run(`service`, func(t *testing.T) {
					svcListReq := &pb.ServiceListRequest{
						Namespace: resource.Namespace,
						Resource:  resource.Name,
					}
					var svcListResp *pb.ServiceListResponse
					// 10s - service is created and active
					require.True(t, await(10, 100, func() bool {
						svcListResp, err = ctrlClient.ServiceList(ctx, svcListReq)
						return err == nil &&
							svcListResp.Code == internal.ResultSuccess &&
							len(svcListResp.Item) > 0 &&
							svcListResp.Item[0].Status == internal.Status_Active
					}), "%#v, %#v", svcListResp, err)
					require.Equal(t, 1, len(svcListResp.Item))
					require.Equal(t, service.Type, svcListResp.Item[0].Type)
				})
				t.Run(`component`, func(t *testing.T) {
					compListReq := &pb.ComponentListRequest{
						Namespace: resource.Namespace,
						Resource:  resource.Name,
					}
					var compListResp *pb.ComponentListResponse
					// 10s - component is created and active
					require.True(t, await(10, 100, func() bool {
						compListResp, err = ctrlClient.ComponentList(ctx, compListReq)
						return err == nil &&
							compListResp.Code == internal.ResultSuccess &&
							len(compListResp.Item) > 0 &&
							compListResp.Item[0].Status == internal.Status_Active
					}), err)
					require.Equal(t, 1, len(compListResp.Item))
					require.Equal(t, component.Type, compListResp.Item[0].Type)
				})
			})
		})
		var c = &http.Client{}
		var res *http.Response
		t.Run(`healthcheck`, func(t *testing.T) {
			assert.True(t, await(5, 10, func() bool {
				res, err = c.Get(fmt.Sprintf("http://%s/health", addr))
				return err == nil && res.StatusCode == 200
			}), "timeout")
			require.Nil(t, err, err)
			require.NotNil(t, res)
			body, err := io.ReadAll(res.Body)
			require.Equal(t, http.StatusOK, res.StatusCode, "%s", string(body))
			require.Nil(t, err, err)
			require.Equal(t, `{"health":"true","reason":""}`, string(body))
			defer res.Body.Close()
		})
	})
	host.logger.Infof("Stopping")
	for i := range len(hosts) {
		hosts[i].logger.Infof("Stopping cluster %d", i)
		hosts[i].ctrlCluster.Stop()
	}
	// TODO - Fix graceful shutdown hang
	// for i := range len(agents) {
	// 	logger.Infof("Stopping agent %d", i)
	// 	agents[i].Stop()
	// }
}

// Wait d seconds, polling n times
func await(d, n time.Duration, fn func() bool) bool {
	for range n {
		if fn() {
			return true
		}
		time.Sleep(d * time.Second / n)
	}
	return false
}
