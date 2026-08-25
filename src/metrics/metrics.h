#ifndef METRICS_H
#define METRICS_H
#include <stdint.h>
typedef struct { uint64_t accepted, rejected, requests; } server_metrics_t;
void metrics_record(server_metrics_t *metrics, int accepted);
void metrics_snapshot(const server_metrics_t *metrics, server_metrics_t *out);
#endif
