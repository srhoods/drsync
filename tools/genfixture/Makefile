# Standalone — no drsync sources, so it builds on a filer host with just a
# compiler. `make static` for a binary you can scp to a host without dev libs.
CC     ?= cc
CFLAGS ?= -O2 -std=gnu11 -Wall -Wextra

genfixture: genfixture.c
	$(CC) $(CFLAGS) -o $@ $<

static: genfixture.c
	$(CC) $(CFLAGS) -static -o genfixture $<

clean:
	rm -f genfixture

.PHONY: static clean
