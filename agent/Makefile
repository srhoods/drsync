CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -g -Wall -Wextra -Wshadow -D_GNU_SOURCE -Ivendor
LDFLAGS ?= -pthread -lzstd -lssl -lcrypto

SRC := src/pb.c src/msgs.c src/wire.c src/tls.c src/state.c src/uring.c src/copy.c src/chunk.c src/xattr.c src/jrn.c src/delete.c src/verify.c src/filter.c src/probe.c src/walker.c src/main.c
OBJ := $(SRC:.c=.o)
BIN := bin/drsync-agent

.PHONY: all clean test

all: $(BIN)

$(BIN): $(OBJ)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

%.o: %.c src/agent.h src/msgs.h src/pb.h src/wire.h src/filter.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Standalone unit test for the glob matcher (no fleet needed).
test: bin/filter_test
	./bin/filter_test

bin/filter_test: src/filter.c src/filter.h test/filter_test.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/filter.c test/filter_test.c

clean:
	rm -f $(OBJ) $(BIN) bin/filter_test
