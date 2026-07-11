/* OpenSSL client transport for the agent → coordinator control connection.
 * mTLS: the agent presents its client cert and verifies the coordinator's
 * server cert (chain + hostname/IP) against the shared CA. TLS 1.3 only. */
#ifndef DRSYNC_TLS_H
#define DRSYNC_TLS_H

#include <stddef.h>
#include <stdint.h>

/* Build the process-global client context from PEM paths. Returns 0 on success,
 * -1 on failure (logs the OpenSSL error). After this, tls_enabled() is true. */
int tls_client_init(const char *ca_path, const char *cert_path, const char *key_path);

/* True once tls_client_init succeeded. */
int tls_enabled(void);

/* Wrap a connected fd in a TLS session, verifying the peer against the CA and
 * matching host (a DNS name or IP literal). Returns an opaque SSL* or NULL on
 * handshake/verify failure (logs). The fd is not closed on failure. */
void *tls_connect(int fd, const char *host);

/* SSL_shutdown + SSL_free. Safe on NULL. */
void tls_close(void *ssl);

/* Blocking full read/write over an established session, used by wire.c.
 * Return 0 on success, -1 on error (errno set; errno==0 on clean peer close). */
int tls_read_full(void *ssl, uint8_t *p, size_t n);
int tls_write_full(void *ssl, const uint8_t *p, size_t n);

#endif
