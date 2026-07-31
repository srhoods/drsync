CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -g -Wall -Wextra -Wshadow -D_GNU_SOURCE -Ivendor
LDFLAGS ?= -pthread -lzstd -lssl -lcrypto

SRC := src/pb.c src/msgs.c src/wire.c src/tls.c src/state.c src/uring.c src/copy.c src/poolsize.c src/ucopy.c src/chunk.c src/xattr.c src/jrn.c src/delete.c src/verify.c src/filter.c src/probe.c src/dirfix.c src/link.c src/tempname.c src/walker.c src/main.c
OBJ := $(SRC:.c=.o)
BIN := bin/drsync-agent

.PHONY: all clean test filter-test fidelity-test ucopy-test tempname-test xattr-copy-test poolsize-test state-test opts-test estat-test link-test

all: $(BIN)

$(BIN): $(OBJ)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

%.o: %.c src/agent.h src/msgs.h src/pb.h src/wire.h src/filter.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Unit tests that need no coordinator/fleet.
test: filter-test fidelity-test xattr-copy-test ucopy-test tempname-test poolsize-test state-test opts-test estat-test link-test

# Standalone unit test for the glob matcher.
filter-test: bin/filter_test
	./bin/filter_test

bin/filter_test: src/filter.c src/filter.h test/filter_test.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/filter.c test/filter_test.c

# walk_fidelity journals a JR_FIDELITY_EXCEPTION (decodes the in-memory buffer;
# the flush path that would need the socket is never taken for one record).
fidelity-test: bin/fidelity_test
	./bin/fidelity_test

bin/fidelity_test: test/fidelity_test.c src/xattr.o src/jrn.o src/msgs.o src/pb.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/fidelity_test.c src/xattr.o src/jrn.o src/msgs.o src/pb.o -pthread -lzstd

# File-copy xattr path: fresh-destination copy + reconcile of a pre-existing one.
xattr-copy-test: bin/xattr_copy_test
	./bin/xattr_copy_test

bin/xattr_copy_test: test/xattr_copy_test.c src/xattr.o src/jrn.o src/msgs.o src/pb.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/xattr_copy_test.c src/xattr.o src/jrn.o src/msgs.o src/pb.o -pthread -lzstd

# ucopy engine: byte-exact copies + in-order sink across edge sizes (memfds).
ucopy-test: bin/ucopy_test
	./bin/ucopy_test

bin/ucopy_test: test/ucopy_test.c src/ucopy.o src/uring.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/ucopy_test.c src/ucopy.o src/uring.o

# Temp-name format: the round trip that keeps a live chunk temp out of the
# orphan sweep (and legacy/foreign names in it).
tempname-test: bin/tempname_test
	./bin/tempname_test

bin/tempname_test: src/tempname.c test/tempname_test.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/tempname.c test/tempname_test.c

# Copy-pool reserve sizing under work-stealing: the throughput fix for the
# lease-requeue-under-stealing bug (see poolsize.c).
poolsize-test: bin/poolsize_test
	./bin/poolsize_test

bin/poolsize_test: src/poolsize.c test/poolsize_test.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/poolsize.c test/poolsize_test.c

# Lease table (O(1) add/start/remove/snapshot/inflight/job_held): the fix for
# the lease-requeue bug at high walker thread counts (see state.c).
state-test: bin/state_test
	./bin/state_test

bin/state_test: src/state.o src/uring.o src/pb.o test/state_test.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/state_test.c src/state.o src/uring.o src/pb.o -pthread

# Job options table (opts_store/opts_get): GPFS-detection flag (no_uring_copy)
# defaults false on an ordinary filesystem and survives a tunables-only
# update, see state.c's fstatfs check.
opts-test: bin/opts_test
	./bin/opts_test

bin/opts_test: src/state.o src/uring.o src/pb.o test/opts_test.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/opts_test.c src/state.o src/uring.o src/pb.o -pthread

# struct estat's dev field (agent.h) and its NLINK_DUP journal encoding
# (msgs.c enc_stat_info field 9) — (dev,ino) is the key the coordinator's
# duplication-cost aggregation and (later) the hardlink registry rely on.
estat-test: bin/estat_test
	./bin/estat_test

bin/estat_test: test/estat_test.c src/uring.o src/jrn.o src/msgs.o src/pb.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/estat_test.c src/uring.o src/jrn.o src/msgs.o src/pb.o -pthread -lzstd

# do_linkfix (link.c): anchor gen re-check, linkat, EEXIST retry (already-
# linked idempotency vs. stale-entry replace) — the LINKFIX phase's executor
# core, factored out so it's testable without opts_get/journal-ack/lease.
link-test: bin/link_test
	./bin/link_test

bin/link_test: test/link_test.c src/link.o src/delete.o src/jrn.o src/msgs.o src/pb.o src/state.o src/uring.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/link_test.c src/link.o src/delete.o src/jrn.o src/msgs.o src/pb.o src/state.o src/uring.o -pthread -lzstd

clean:
	rm -f $(OBJ) $(BIN) bin/filter_test bin/fidelity_test bin/xattr_copy_test bin/ucopy_test bin/tempname_test bin/poolsize_test bin/state_test bin/opts_test bin/estat_test bin/link_test
