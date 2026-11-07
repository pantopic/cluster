package main

import (
	"github.com/pantopic/ext-raft/sdk-go"

	"github.com/pantopic/cluster/test/module/storage/pb"
)

var (
	register []byte
)

func init() {
	raft.Concurrent(update, finish, read)
}

func main() {}

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	copy(register, cmd)
	return
}

func finish() {}

func read(query []byte) (value uint64, data []byte) {
	out, _ := (&testpb.PingResponse{}).MarshalVT()
	return 0, out
}
