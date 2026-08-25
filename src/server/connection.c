#include "connection.h"
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

/* Protocol: each request is an unsigned decimal delta followed by '\n'; response is A\n or R\n. */
void *connection_run(void *opaque) {
    connection_args_t *args = opaque;
    char buf[4096]; size_t used = 0;
    for (;;) {
        ssize_t n = read(args->fd, buf + used, sizeof(buf) - used);
        if (n <= 0) break;
        used += (size_t)n; size_t start = 0;
        for (size_t i = 0; i < used; ++i) {
            if (buf[i] != '\n') continue;
            buf[i] = '\0'; char *end = NULL; errno = 0;
            unsigned long long parsed = strtoull(buf + start, &end, 10);
            char reply[2] = {'R', '\n'};
            if (errno == 0 && end != buf + start && *end == '\0') {
                uint64_t now;
                int accepted = incrnchk(counter_store_primary(args->store), (uint64_t)parsed, &now) == INCRNCHK_ACCEPTED;
                metrics_record(args->metrics, accepted); reply[0] = accepted ? 'A' : 'R';
            }
            if (write(args->fd, reply, sizeof(reply)) != sizeof(reply)) { close(args->fd); free(args); return NULL; }
            start = i + 1;
        }
        if (start) { used -= start; for (size_t i = 0; i < used; ++i) buf[i] = buf[start + i]; }
        if (used == sizeof(buf)) break;
    }
    close(args->fd); free(args); return NULL;
}
