#ifndef CONNECTION_H
#define CONNECTION_H
#include "counter_store.h"
#include "metrics.h"
typedef struct { int fd; counter_store_t *store; server_metrics_t *metrics; } connection_args_t;
void *connection_run(void *opaque);
#endif
