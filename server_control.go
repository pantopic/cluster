package cluster

import (
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/logbn/zongzi"

	"github.com/pantopic/cluster/internal"
	"github.com/pantopic/cluster/pb"
)

type serverControl struct {
	addr       string
	agent      *zongzi.Agent
	grpcServer *grpc.Server
}

func newServerControl(agent *zongzi.Agent, addr string) *serverControl {
	grpcServer := grpc.NewServer([]grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Hour,
			Timeout: 20 * time.Second,
		}),
	}...)
	return &serverControl{
		addr:       addr,
		agent:      agent,
		grpcServer: grpcServer,
	}

}

func (s *serverControl) Start() {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic(err)
	}
	shardClient := s.agent.Client(uint64(ShardID))
	pb.RegisterControlServer(s.grpcServer, internal.NewGrpcServiceControl(shardClient))
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
}

func (s *serverControl) Stop() {
	s.grpcServer.Stop()
}

func (s *serverControl) Client() pb.ControlClient {
	conn, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewControlClient(conn)
}
