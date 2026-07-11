/* Frame codec on a connected transport: u32-LE length, u16-LE type, payload.
 * The transport is either a plain fd or a TLS session (see tls.h). */
#ifndef DRSYNC_WIRE_H
#define DRSYNC_WIRE_H

#include <stddef.h>
#include <stdint.h>

#define WIRE_MAX_FRAME (16u << 20)

/* One coordinator connection. ssl is an opaque SSL* (void* to keep OpenSSL out
 * of this header); NULL means plaintext over fd. fd stays valid even under TLS
 * so the control loop can poll() it for readiness. */
struct conn {
    int   fd;
    void *ssl;
};

/* Blocking full-frame write; returns 0 or -1 (errno set). */
int wire_write(struct conn *c, uint16_t type, const uint8_t *payload, size_t len);

/* Blocking full-frame read. *payload is malloc'd (caller frees). Returns 0,
 * or -1 on error/EOF (errno 0 on clean EOF). */
int wire_read(struct conn *c, uint16_t *type, uint8_t **payload, size_t *len);

#endif
