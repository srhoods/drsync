# Standalone — no drsync sources, so it builds on a filer host with just a
# compiler. `make static` for a binary you can scp to a host without dev libs.
CC     ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -pthread

fsprobe: fsprobe.c
	$(CC) $(CFLAGS) -o $@ $<

static: fsprobe.c
	$(CC) $(CFLAGS) -static -o fsprobe $<

clean:
	rm -f fsprobe

.PHONY: static clean
