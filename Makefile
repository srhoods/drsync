# drsync top-level build

GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

# `agent` must be here: it names a target whose recipe builds the C agent, but
# it is also the name of a directory. Without .PHONY, make sees that directory,
# considers the target up to date, and silently skips the build — so on a fresh
# clone `make build` produced no agent binary and e2e failed at agent launch.
.PHONY: all proto build agent fsprobe genfixture test test-all webui-test e2e vet clean tools

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

# Standalone filesystem profiler for diagnosing slow destinations (GPFS/Weka).
# Not part of `build`: it ships to filer hosts on its own, and depends on
# nothing else in the tree. See tools/fsprobe/README.md.
fsprobe:
	$(MAKE) -C tools/fsprobe
	@mkdir -p bin && cp tools/fsprobe/fsprobe bin/

# Synthetic fidelity-test tree generator (docs/DESIGN-agent.md §9). Also
# standalone; ships to a source host to build the tree a real sync is tested
# against. See tools/genfixture/README.md.
genfixture:
	$(MAKE) -C tools/genfixture
	@mkdir -p bin && cp tools/genfixture/genfixture bin/

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
