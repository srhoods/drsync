# drsync top-level build

GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: all proto build test test-all webui-test vet clean tools

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

# The console tests need node; `test` stays Go-only so a Go toolchain alone is
# still enough to build and check the coordinator. Use `test-all` for both.
test-all: test webui-test

webui-test:
	@command -v node >/dev/null 2>&1 || { \
	  echo "webui-test: node not found — install Node >= 20, or skip (this target is not part of \`make test\`)"; \
	  exit 1; }
	@[ -d webui/test/node_modules ] || npm --prefix webui/test install --no-audit --no-fund
	npm --prefix webui/test test

e2e: build
	./test/e2e.sh

vet:
	go vet ./...

clean:
	rm -rf bin
