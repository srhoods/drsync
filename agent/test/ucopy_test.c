/* Unit test for the io_uring copy engine (ucopy.c): byte-exact copies across
 * edge-case sizes, a correct byte count returned, and the sink fed the whole
 * stream in order (the property the inline hash relies on). Uses memfds for
 * src/dst — regular-file semantics, io_uring-compatible.
 *
 * Build+run: `make ucopy-test` in agent/. Skips cleanly (exit 0) when io_uring
 * is unavailable, since the engine then legitimately does not run.
 */
#include "../src/agent.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

void log_line(const char *level, const char *fmt, ...)
{
    (void)level;
    (void)fmt;
}

static int failures;
#define CHECK(cond, ...)                  \
    do {                                  \
        if (!(cond)) {                    \
            fprintf(stderr, "FAIL: ");    \
            fprintf(stderr, __VA_ARGS__); \
            fprintf(stderr, "\n");        \
            failures++;                   \
        }                                 \
    } while (0)

/* Sink that appends the delivered bytes, so we can prove the whole stream was
 * fed in order. */
struct cap {
    uint8_t *buf;
    size_t   len, cap;
};
static void cap_sink(void *arg, const void *data, size_t n)
{
    struct cap *c = arg;
    memcpy(c->buf + c->len, data, n); /* c->buf is sized to the file */
    c->len += n;
}

static int memfd(const char *name)
{
    return (int)syscall(SYS_memfd_create, name, 0u);
}

static void run_size(size_t size)
{
    uint8_t *src = malloc(size ? size : 1);
    for (size_t i = 0; i < size; i++)
        src[i] = (uint8_t)(i * 1103515245u + 12345u); /* deterministic pattern */

    int in = memfd("ucopy-src"), out = memfd("ucopy-dst");
    if (in < 0 || out < 0) {
        fprintf(stderr, "FAIL: memfd_create: %s\n", strerror(errno));
        failures++;
        free(src);
        return;
    }
    for (size_t off = 0; off < size;) {
        ssize_t w = pwrite(in, src + off, size - off, (off_t)off);
        if (w <= 0) {
            fprintf(stderr, "FAIL: seed pwrite\n");
            failures++;
            goto done;
        }
        off += (size_t)w;
    }

    struct cap c = { .buf = malloc(size ? size : 1), .len = 0, .cap = size };
    int64_t rc = ucopy_run(in, out, size, cap_sink, &c);

    CHECK(rc == (int64_t)size, "size %zu: ucopy_run returned %lld, want %zu",
          size, (long long)rc, size);
    CHECK(c.len == size, "size %zu: sink saw %zu bytes, want %zu", size, c.len, size);
    CHECK(size == 0 || memcmp(c.buf, src, size) == 0,
          "size %zu: sink bytes differ from source (order/content)", size);

    uint8_t *got = malloc(size ? size : 1);
    for (size_t off = 0; off < size;) {
        ssize_t r = pread(out, got + off, size - off, (off_t)off);
        if (r <= 0) {
            fprintf(stderr, "FAIL: size %zu: readback\n", size);
            failures++;
            break;
        }
        off += (size_t)r;
    }
    CHECK(size == 0 || memcmp(got, src, size) == 0,
          "size %zu: destination differs from source", size);
    free(got);
    free(c.buf);
done:
    if (in >= 0)
        close(in);
    if (out >= 0)
        close(out);
    free(src);
}

int main(void)
{
    uring_probe(true);
    if (!ucopy_available()) {
        printf("SKIP: io_uring copy engine unavailable in this environment\n");
        return 0;
    }

    const size_t B = 1 << 20; /* UCOPY_BUF_SIZE */
    size_t sizes[] = { 0, 1, 4095, 4096, B - 1, B, B + 1, 2 * B,
                       3 * B + 123, 5 * B - 7 };
    for (size_t i = 0; i < sizeof sizes / sizeof *sizes; i++)
        run_size(sizes[i]);

    if (failures) {
        fprintf(stderr, "%d ucopy test(s) failed\n", failures);
        return 1;
    }
    printf("all ucopy tests passed\n");
    return 0;
}
