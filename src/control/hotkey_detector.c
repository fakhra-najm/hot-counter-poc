#include "hotkey_detector.h"
#include <stdio.h>
#include <time.h>
static uint64_t now_ns(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (uint64_t)t.tv_sec * 1000000000ULL + t.tv_nsec; }
static void *monitor(void *opaque) {
    hotkey_detector_t *d = opaque; uint64_t previous = 0, at = now_ns();
    while (!atomic_load_explicit(&d->stop, memory_order_relaxed)) {
        struct timespec pause = {.tv_sec = d->poll_ms / 1000, .tv_nsec = (long)(d->poll_ms % 1000) * 1000000L}; nanosleep(&pause, NULL);
        server_metrics_t snap; metrics_snapshot(d->metrics, &snap); uint64_t next = now_ns(), elapsed = next - at;
        uint64_t tps = elapsed ? (snap.requests - previous) * 1000000000ULL / elapsed : 0;
        if (tps >= d->threshold_tps && router_get_mode(d->router) == ROUTE_NORMAL) { router_set_mode(d->router, ROUTE_PEAK); fprintf(stderr, "control-plane mode=PEAK sampled_tps=%llu poll_ms=%u\n", (unsigned long long)tps, d->poll_ms); }
        previous = snap.requests; at = next;
    }
    return NULL;
}
int hotkey_detector_start(hotkey_detector_t *d, const server_metrics_t *metrics, router_t *router, uint64_t threshold_tps, unsigned poll_ms) {
    *d = (hotkey_detector_t){.metrics=metrics,.router=router,.threshold_tps=threshold_tps,.poll_ms=poll_ms}; atomic_init(&d->stop, 0); return pthread_create(&d->thread, NULL, monitor, d);
}
void hotkey_detector_stop(hotkey_detector_t *d) { atomic_store(&d->stop, 1); pthread_join(d->thread, NULL); }
