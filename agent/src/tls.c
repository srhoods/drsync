#include "tls.h"
#include "agent.h"

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

static SSL_CTX *g_ctx;

static void log_ssl_err(const char *what)
{
    unsigned long e = ERR_get_error();
    char buf[256];
    if (e) {
        ERR_error_string_n(e, buf, sizeof buf);
        LOGE("%s: %s", what, buf);
    } else {
        LOGE("%s: %s", what, strerror(errno));
    }
}

int tls_client_init(const char *ca_path, const char *cert_path, const char *key_path)
{
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) {
        log_ssl_err("SSL_CTX_new");
        return -1;
    }
    /* TLS 1.3 only — matches the coordinator's MinVersion. */
    if (!SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION)) {
        log_ssl_err("set_min_proto_version");
        goto fail;
    }
    if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) != 1) {
        log_ssl_err("load CA");
        goto fail;
    }
    if (SSL_CTX_use_certificate_chain_file(ctx, cert_path) != 1) {
        log_ssl_err("load client cert");
        goto fail;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) != 1) {
        log_ssl_err("load client key");
        goto fail;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        log_ssl_err("client key/cert mismatch");
        goto fail;
    }
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
    g_ctx = ctx;
    return 0;
fail:
    SSL_CTX_free(ctx);
    return -1;
}

int tls_enabled(void)
{
    return g_ctx != NULL;
}

void *tls_connect(int fd, const char *host)
{
    SSL *ssl = SSL_new(g_ctx);
    if (!ssl) {
        log_ssl_err("SSL_new");
        return NULL;
    }
    if (SSL_set_fd(ssl, fd) != 1) {
        log_ssl_err("SSL_set_fd");
        SSL_free(ssl);
        return NULL;
    }

    /* Enforce that the coordinator cert actually names the endpoint we dialed.
     * IP literals go through the IP matcher; hostnames through the DNS matcher
     * plus SNI (SNI must not carry a bare IP). */
    X509_VERIFY_PARAM *param = SSL_get0_param(ssl);
    X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    unsigned char ipbuf[16];
    if (inet_pton(AF_INET, host, ipbuf) == 1 || inet_pton(AF_INET6, host, ipbuf) == 1) {
        if (X509_VERIFY_PARAM_set1_ip_asc(param, host) != 1) {
            log_ssl_err("set verify IP");
            SSL_free(ssl);
            return NULL;
        }
    } else {
        if (X509_VERIFY_PARAM_set1_host(param, host, 0) != 1) {
            log_ssl_err("set verify host");
            SSL_free(ssl);
            return NULL;
        }
        SSL_set_tlsext_host_name(ssl, host); /* SNI */
    }

    if (SSL_connect(ssl) != 1) {
        long v = SSL_get_verify_result(ssl);
        if (v != X509_V_OK)
            LOGE("TLS verify failed: %s", X509_verify_cert_error_string(v));
        else
            log_ssl_err("TLS handshake");
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}

void tls_close(void *ssl)
{
    if (!ssl)
        return;
    SSL_shutdown((SSL *)ssl);
    SSL_free((SSL *)ssl);
}

int tls_read_full(void *v, uint8_t *p, size_t n)
{
    SSL *ssl = v;
    while (n > 0) {
        int r = SSL_read(ssl, p, n > INT_MAX ? INT_MAX : (int)n);
        if (r > 0) {
            p += r;
            n -= (size_t)r;
            continue;
        }
        int err = SSL_get_error(ssl, r);
        if (err == SSL_ERROR_ZERO_RETURN) {
            errno = 0; /* clean TLS close_notify */
            return -1;
        }
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
            continue; /* blocking socket: retry */
        if (err == SSL_ERROR_SYSCALL && errno == EINTR)
            continue;
        if (errno == 0)
            errno = EIO;
        return -1;
    }
    return 0;
}

int tls_write_full(void *v, const uint8_t *p, size_t n)
{
    SSL *ssl = v;
    while (n > 0) {
        int w = SSL_write(ssl, p, n > INT_MAX ? INT_MAX : (int)n);
        if (w > 0) {
            p += w;
            n -= (size_t)w;
            continue;
        }
        int err = SSL_get_error(ssl, w);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
            continue;
        if (err == SSL_ERROR_SYSCALL && errno == EINTR)
            continue;
        if (errno == 0)
            errno = EIO;
        return -1;
    }
    return 0;
}
