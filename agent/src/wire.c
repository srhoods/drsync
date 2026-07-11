#include "wire.h"
#include "tls.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int write_full(struct conn *c, const uint8_t *p, size_t n)
{
    if (c->ssl)
        return tls_write_full(c->ssl, p, n);
    while (n > 0) {
        ssize_t w = write(c->fd, p, n);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static int read_full(struct conn *c, uint8_t *p, size_t n)
{
    if (c->ssl)
        return tls_read_full(c->ssl, p, n);
    while (n > 0) {
        ssize_t r = read(c->fd, p, n);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (r == 0) {
            errno = 0; /* clean EOF */
            return -1;
        }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

int wire_write(struct conn *c, uint16_t type, const uint8_t *payload, size_t len)
{
    if (len > WIRE_MAX_FRAME) {
        errno = EMSGSIZE;
        return -1;
    }
    uint8_t hdr[6];
    hdr[0] = (uint8_t)(len);
    hdr[1] = (uint8_t)(len >> 8);
    hdr[2] = (uint8_t)(len >> 16);
    hdr[3] = (uint8_t)(len >> 24);
    hdr[4] = (uint8_t)(type);
    hdr[5] = (uint8_t)(type >> 8);
    /* Two writes are fine: the control thread is the only socket writer. */
    if (write_full(c, hdr, sizeof hdr) < 0)
        return -1;
    return write_full(c, payload, len);
}

int wire_read(struct conn *c, uint16_t *type, uint8_t **payload, size_t *len)
{
    uint8_t hdr[6];
    if (read_full(c, hdr, sizeof hdr) < 0)
        return -1;
    uint32_t n = (uint32_t)hdr[0] | (uint32_t)hdr[1] << 8 |
                 (uint32_t)hdr[2] << 16 | (uint32_t)hdr[3] << 24;
    *type = (uint16_t)((uint16_t)hdr[4] | (uint16_t)hdr[5] << 8);
    if (n > WIRE_MAX_FRAME) {
        errno = EMSGSIZE;
        return -1;
    }
    uint8_t *buf = malloc(n ? n : 1);
    if (!buf)
        return -1;
    if (n && read_full(c, buf, n) < 0) {
        int e = errno;
        free(buf);
        errno = e;
        return -1;
    }
    *payload = buf;
    *len = n;
    return 0;
}
