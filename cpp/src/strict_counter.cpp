#include "counter_poc/strict_counter.hpp"
namespace counter_poc {
StrictCounter::StrictCounter(Amount limit, Amount initial) noexcept : value_(initial), limit_(limit) {}
Result StrictCounter::apply(Amount delta) noexcept {
    Amount current=value_.load(std::memory_order_relaxed);
    for (;;) {
        if (current>limit_ || delta>limit_-current) return {Decision::Rejected,current,0};
        const Amount next=current+delta;
        if (value_.compare_exchange_weak(current,next,std::memory_order_acq_rel,std::memory_order_relaxed))
            return {Decision::Accepted,next,0};
    }
}
Amount StrictCounter::value() const noexcept { return value_.load(std::memory_order_acquire); }
bool StrictCounter::seed_after_quiescence(Amount value) noexcept {
    if (value > limit_) return false;
    value_.store(value, std::memory_order_release);
    return true;
}
}  // namespace counter_poc
