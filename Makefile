dev:
	@go build -ldflags="-s -w" -o _dist/pantopic && cd cmd/standalone && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/pantopic

test:
	@go test -v

integration:
	@go test ./...

bench:
	@go test -bench=. -run=_ -v

unit:
	@go test ./... -tags unit -v

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

gen:
	@protoc proto/*.proto \
		--go_out=pb \
		--go_opt=paths=source_relative \
		--go-grpc_out=pb \
		--go-grpc_opt=paths=source_relative \
		-I proto

gen-test:
	@protoc test/proto/*.proto \
		--go_out=test/pb \
		--go_opt=paths=source_relative \
		--go-grpc_out=test/pb \
		--go-grpc_opt=paths=source_relative \
		-I test/proto
	@protoc test/proto/*.proto \
		--plugin protoc-gen-go-lite="${GOBIN}/protoc-gen-go-lite" \
		--go-lite_out=test/module/service-grpc/pb  \
		--go-lite_opt=paths=source_relative \
		--go-lite_opt=features=marshal+unmarshal+size \
		-I test/proto
	@cp test/module/service-grpc/pb/* test/module/storage/pb

gen-lite-install:
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@latest

cloc:
	@cloc . --exclude-dir=_example,_dist,internal,cmd --exclude-ext=pb.go

wasm-storage:
	@cd test/module/storage && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=conservative -scheduler=none -o ../../../_dist/storage.wasm -no-debug
wasm-storage-dev:
	@cd test/module/storage && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../../../_dist/storage.dev.wasm
wasm-service-grpc:
	@cd test/module/service-grpc && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=conservative -scheduler=none -o ../../../_dist/service-grpc.wasm -no-debug
wasm-service-grpc-dev:
	@cd test/module/service-grpc && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../../../_dist/service-grpc.dev.wasm
wasm-control:
	@cd test/module/control && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=conservative -scheduler=none -o ../../../_dist/control.wasm -no-debug
wasm-control-dev:
	@cd test/module/control && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o ../../../_dist/control.dev.wasm
wasm-dev: wasm-storage-dev wasm-service-grpc-dev wasm-control-dev
wasm-prod: wasm-storage wasm-service-grpc wasm-control
wasm: wasm-dev wasm-prod

.PHONY: test
