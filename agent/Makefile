CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -g -Wall -Wextra -Wshadow -D_GNU_SOURCE -Ivendor
LDFLAGS ?= -pthread -lzstd -lssl -lcrypto

SRC := src/pb.c src/msgs.c src/wire.c src/tls.c src/state.c src/uring.c src/copy.c src/ucopy.c src/chunk.c src/xattr.c src/jrn.c src/delete.c src/verify.c src/walker.c src/main.c
OBJ := $(SRC:.c=.o)
BIN := bin/drsync-agent

.PHONY: all clean test ucopy-test

all: $(BIN)

$(BIN): $(OBJ)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

%.o: %.c src/agent.h src/msgs.h src/pb.h src/wire.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Unit tests that need no coordinator/fleet.
test: ucopy-test

# ucopy engine: byte-exact copies + in-order sink across edge sizes (memfds).
ucopy-test: bin/ucopy_test
	./bin/ucopy_test

bin/ucopy_test: test/ucopy_test.c src/ucopy.o src/uring.o
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ test/ucopy_test.c src/ucopy.o src/uring.o

clean:
	rm -f $(OBJ) $(BIN) bin/ucopy_test
