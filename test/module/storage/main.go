package main

import (
	"github.com/pantopic/ext-raft/sdk-go"
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
	data = register
	return
}
