#include "incrnchk.h"

void counter_init(counter_t *counter, uint64_t limit) {
    __atomic_store_n(&counter->value, 0, __ATOMIC_RELAXED);
    counter->limit = limit;
}

incrnchk_result_t incrnchk(counter_t *counter, uint64_t delta, uint64_t *new_value) {
    uint64_t observed = __atomic_load_n(&counter->value, __ATOMIC_RELAXED);
    for (;;) {
        /* Written this way to avoid unsigned addition overflow. */
        if (delta > counter->limit - observed) {
            if (new_value) *new_value = observed;
            return INCRNCHK_REJECTED;
        }
        const uint64_t desired = observed + delta;
        if (__atomic_compare_exchange_n(&counter->value, &observed, desired, false,
                                        __ATOMIC_SEQ_CST, __ATOMIC_RELAXED)) {
            if (new_value) *new_value = desired;
            return INCRNCHK_ACCEPTED;
        }
    }
}

uint64_t counter_value(const counter_t *counter) {
    return __atomic_load_n(&counter->value, __ATOMIC_SEQ_CST);
}
