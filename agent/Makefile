CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -g -Wall -Wextra -Wshadow -D_GNU_SOURCE -Ivendor
LDFLAGS ?= -pthread -lzstd -lssl -lcrypto

SRC := src/pb.c src/msgs.c src/wire.c src/tls.c src/state.c src/uring.c src/copy.c src/ucopy.c src/chunk.c src/xattr.c src/jrn.c src/delete.c src/verify.c src/filter.c src/probe.c src/dirfix.c src/tempname.c src/walker.c src/main.c
OBJ := $(SRC:.c=.o)
BIN := bin/drsync-agent

.PHONY: all clean test filter-test fidelity-test ucopy-test tempname-test xattr-copy-test

all: $(BIN)

$(BIN): $(OBJ)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

%.o: %.c src/agent.h src/msgs.h src/pb.h src/wire.h src/filter.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Unit tests that need no coordinator/fleet.
test: filter-test fidelity-test xattr-copy-test ucopy-test tempname-test

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

clean:
	rm -f $(OBJ) $(BIN) bin/filter_test bin/fidelity_test bin/xattr_copy_test bin/ucopy_test bin/tempname_test
