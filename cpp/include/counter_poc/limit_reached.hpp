#pragma once

#include <atomic>

namespace counter_poc {

// An edge may cache only a definitive exhausted state. It never stores an
// ordinary value, so stale cache data cannot authorize an additional spend.
class LimitReachedCache final {
public:
    bool known_exhausted() const noexcept { return exhausted_.load(std::memory_order_acquire); }

    // Returns true exactly once, for the transition to exhausted.
    bool mark_exhausted() noexcept {
        bool expected = false;
        return exhausted_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire);
    }

private:
    std::atomic<bool> exhausted_{false};
};

}  // namespace counter_poc
