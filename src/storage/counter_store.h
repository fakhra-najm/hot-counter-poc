#ifndef COUNTER_STORE_H
#define COUNTER_STORE_H

#include "incrnchk.h"

typedef struct { counter_t primary; } counter_store_t;
void counter_store_init(counter_store_t *store, uint64_t limit);
counter_t *counter_store_primary(counter_store_t *store);

#endif
