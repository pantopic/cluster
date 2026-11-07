package main

import (
	"github.com/pantopic/ext-grpc-server/sdk-go"
	"github.com/pantopic/ext-grpc-server/sdk-go/codes"
	"github.com/pantopic/ext-shard-client/sdk-go"
)

func init() {
	grpc_server.Init(
		grpc_server.WithBufferCap(64, 1<<16),
	)
	grpc_server.NewService(`testpb.Test`).
		Unary(`Ping`, ping)
	shard_client.RegisterAsyncRecv(asyncRecv)
}

func main() {}

func ping(in []byte) (err error) {
	return shard_client.New([]byte(`test`), []byte(`test`)).ReadAsync([]byte("ping"), in, false)
}

func asyncRecv(name, data []byte, val uint64, err error) {
	if err != nil {
		grpc_server.SendErr(codes.Internal, []byte(err.Error()))
		return
	}
	switch string(name) {
	case "ping":
		grpc_server.Send(data)
	default:
		panic("unknown asyncRecv name: " + string(name))
	}
}
