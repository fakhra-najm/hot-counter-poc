#ifndef INCRNCHK_H
#define INCRNCHK_H

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    _Alignas(64) uint64_t value;
    uint64_t limit;
} counter_t;

typedef enum { INCRNCHK_ACCEPTED, INCRNCHK_REJECTED } incrnchk_result_t;

void counter_init(counter_t *counter, uint64_t limit);
incrnchk_result_t incrnchk(counter_t *counter, uint64_t delta, uint64_t *new_value);
uint64_t counter_value(const counter_t *counter);

#endif
