CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -g -Wall -Wextra -Wshadow -D_GNU_SOURCE -Ivendor
LDFLAGS ?= -pthread -lzstd -lssl -lcrypto

SRC := src/pb.c src/msgs.c src/wire.c src/tls.c src/state.c src/uring.c src/copy.c src/chunk.c src/xattr.c src/jrn.c src/delete.c src/verify.c src/walker.c src/main.c
OBJ := $(SRC:.c=.o)
BIN := bin/drsync-agent

.PHONY: all clean test fidelity-test

all: $(BIN)

$(BIN): $(OBJ)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

%.o: %.c src/agent.h src/msgs.h src/pb.h src/wire.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Unit tests that need no coordinator/fleet.
test: fidelity-test

# walk_fidelity journals a JR_FIDELITY_EXCEPTION (decodes the in-memory buffer;
# the flush path that would need the socket is never taken for one record).
fidelity-test: bin/fidelity_test
	./bin/fidelity_test

bin/fidelity_test: test/fidelity_test.c src/xattr.o src/jrn.o src/msgs.o src/pb.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/fidelity_test.c src/xattr.o src/jrn.o src/msgs.o src/pb.o -pthread -lzstd

clean:
	rm -f $(OBJ) $(BIN) bin/fidelity_test
