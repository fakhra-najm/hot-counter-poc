#include "counter_store.h"
void counter_store_init(counter_store_t *store, uint64_t limit) { counter_init(&store->primary, limit); }
counter_t *counter_store_primary(counter_store_t *store) { return &store->primary; }
