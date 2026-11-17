dev:
	@go build -ldflags="-s -w" -o _dist/pantopic cmd/standalone && cd cmd/standalone && docker compose up --build

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
	@mkdir -p test/pb
	@mkdir -p test/module/service-grpc/pb
	@mkdir -p test/module/storage/pb
	@mkdir -p test/module/control/pb
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
	@cp test/module/service-grpc/pb/* test/module/control/pb

gen-lite-install:
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@latest

cloc:
	@cloc . --exclude-dir=_example,_dist,internal,cmd --exclude-ext=pb.go

WASM_MODULES := storage service-grpc control

define WASM_BUILD_GO
cd test/module/$(1) && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../../../_dist/$(1).dev.wasm && cd ../../..;
cd test/module/$(1) && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../../../_dist/$(1).wasm -no-debug && cd ../../..;
endef

wasm-go:
	$(foreach mod,$(WASM_MODULES),$(call WASM_BUILD_GO,$(mod)))

wasm: wasm-go

.PHONY: test
