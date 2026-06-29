package main

import (
	"github.com/pantopic/wazero-state-machine/sdk-go"
)

var (
	register []byte
)

func init() {
	statemachine.Concurrent(update, finish, read)
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
