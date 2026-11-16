package pantopic

type ModuleRegistry interface {
	RegisterControl(name string, wasm []byte)
	RegisterService(name string, wasm []byte)
	RegisterStorage(name string, wasm []byte)
	RegisterRelay(name string, wasm []byte)
}
