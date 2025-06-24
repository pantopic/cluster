package statemachine

import (
	"unsafe"
)

type (
	openFunc   func() uint64
	updateFunc func(index uint64, cmd []byte) (value uint64, data []byte)
	finishFunc func()
	readFunc   func(query []byte) (value uint64, data []byte)
)

var (
	fnOpen   openFunc
	fnUpdate updateFunc
	fnRead   readFunc
	fnFinish finishFunc
)

func Register(update updateFunc, read readFunc) {
	fnUpdate = update
	fnRead = read
}

func RegisterPersistent(open openFunc, update updateFunc, read readFunc) {
	fnOpen = open
	fnUpdate = update
	fnRead = read
}

var (
	maxReq uint32 = 16 * 1024 * 1024
	maxRes uint32 = 16 * 1024 * 1024
	reqLen uint32
	resLen uint32
	index  uint64
	value  uint64
	req    = make([]byte, int(maxReq))
	res    = make([]byte, int(maxRes))
	meta   = make([]uint32, 8)
)

//export statemachine
func statemachine() uint32 {
	meta[0] = uint32(uintptr(unsafe.Pointer(&maxReq)))
	meta[1] = uint32(uintptr(unsafe.Pointer(&maxRes)))
	meta[2] = uint32(uintptr(unsafe.Pointer(&reqLen)))
	meta[3] = uint32(uintptr(unsafe.Pointer(&resLen)))
	meta[4] = uint32(uintptr(unsafe.Pointer(&index)))
	meta[5] = uint32(uintptr(unsafe.Pointer(&value)))
	meta[6] = uint32(uintptr(unsafe.Pointer(&req[0])))
	meta[7] = uint32(uintptr(unsafe.Pointer(&res[0])))
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

// export statemachineOpen
func open() uint64 {
	return fnOpen()
}

// export statemachineUpdate
func update() {
	var tmp []byte
	value, tmp = fnUpdate(index, req[:int(reqLen)])
	copy(res, tmp)
	resLen = uint32(len(tmp))
}

// export statemachineFinish
func finish() {
	fnFinish()
}

// export statemachineRead
func read() {
	var tmp []byte
	value, tmp = fnRead(req[:int(reqLen)])
	copy(res, tmp)
	resLen = uint32(len(tmp))
}

var _ = statemachine
var _ = open
var _ = update
var _ = finish
var _ = read
