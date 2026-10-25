SHELL := /bin/sh
ARCH ?= $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
GO ?= go
BIN := bin/server-linux

.PHONY: build build-linux up up-cluster down logs e2e e2e-cluster loadtest pushcli proto clean

build:
	$(GO) build ./...

build-linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) $(GO) build -trimpath -ldflags "-s -w" -o $(BIN) ./cmd/server

up: build-linux
	docker compose up -d --build

up-cluster: build-linux
	docker compose --profile cluster up -d --build

down:
	docker compose --profile cluster down

logs:
	docker compose --profile cluster logs -f --tail=100

e2e:
	node scripts/e2e.mjs

e2e-cluster:
	node scripts/e2e-cluster.mjs

loadtest:
	node scripts/loadtest.mjs

pushcli:
	$(GO) run ./cmd/pushcli $(ARGS)

proto:
	protoc -I api --go_out=. --go_opt=module=ws-nchan-system \
		--go-grpc_out=. --go-grpc_opt=module=ws-nchan-system api/push.proto

clean:
	docker compose --profile cluster down -v
	rm -rf bin
