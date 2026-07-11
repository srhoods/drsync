# drsync top-level build

GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: all proto build test vet clean tools

all: proto build test

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install github.com/bufbuild/buf/cmd/buf@latest

proto:
	buf generate

build: agent
	go build ./...
	go build -o bin/drsyncd ./coordinator/cmd/drsyncd
	go build -o bin/drsync-journal ./coordinator/cmd/drsync-journal
	go build -o bin/drsync ./cli/drsync

agent:
	$(MAKE) -C agent
	@mkdir -p bin && cp agent/bin/drsync-agent bin/

test:
	go test ./...

e2e: build
	./test/e2e.sh

vet:
	go vet ./...

clean:
	rm -rf bin
