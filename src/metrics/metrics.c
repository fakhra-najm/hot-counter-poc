#include "metrics.h"
void metrics_record(server_metrics_t *m, int accepted) {
    __atomic_fetch_add(&m->requests, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(accepted ? &m->accepted : &m->rejected, 1, __ATOMIC_RELAXED);
}
void metrics_snapshot(const server_metrics_t *m, server_metrics_t *out) {
    out->requests = __atomic_load_n(&m->requests, __ATOMIC_RELAXED);
    out->accepted = __atomic_load_n(&m->accepted, __ATOMIC_RELAXED);
    out->rejected = __atomic_load_n(&m->rejected, __ATOMIC_RELAXED);
}
