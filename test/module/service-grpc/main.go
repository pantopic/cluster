package main

import (
	"github.com/pantopic/wazero-grpc-server/sdk-go"
)

func init() {
	grpc_server.Init(
		grpc_server.WithBufferCap(64, 1<<16),
	)
	grpc_server.NewService(`test.Test`).
		Unary(`Ping`, ping)
}

func main() {}

func ping(in []byte) (err error) {
	return grpc_server.Send(nil)
}
