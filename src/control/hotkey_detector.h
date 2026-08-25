#ifndef HOTKEY_DETECTOR_H
#define HOTKEY_DETECTOR_H
#include "metrics.h"
#include "router.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
typedef struct { const server_metrics_t *metrics; router_t *router; uint64_t threshold_tps; unsigned poll_ms; _Atomic int stop; pthread_t thread; } hotkey_detector_t;
int hotkey_detector_start(hotkey_detector_t *detector, const server_metrics_t *metrics, router_t *router, uint64_t threshold_tps, unsigned poll_ms);
void hotkey_detector_stop(hotkey_detector_t *detector);
#endif
