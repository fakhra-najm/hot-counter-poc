#include "counter_poc/reserved_counter.hpp"
#include <stdexcept>
namespace counter_poc {
ReservedCounter::ReservedCounter(Amount limit, std::uint32_t count) : limit_(limit) {
    if (!count) throw std::invalid_argument("at least one shard is required");
    const Amount base=limit/count, remainder=limit%count;
    shards_.reserve(count);
    for (std::uint32_t i=0;i<count;++i) { auto shard=std::make_unique<Shard>(); shard->reserve=base+(i<remainder?1:0); shards_.push_back(std::move(shard)); }
}
Result ReservedCounter::apply(std::uint32_t id, Amount delta) noexcept {
    const std::uint32_t selected=id%shards();
    Shard& shard=*shards_[selected]; Amount current=shard.used.load(std::memory_order_relaxed);
    for (;;) {
        if (current>shard.reserve || delta>shard.reserve-current) return {Decision::Rejected,current,selected};
        const Amount next=current+delta;
        if (shard.used.compare_exchange_weak(current,next,std::memory_order_acq_rel,std::memory_order_relaxed)) {
            total_.fetch_add(delta, std::memory_order_relaxed);
            return {Decision::Accepted,next,selected};
        }
    }
}
Amount ReservedCounter::total() const noexcept { return total_.load(std::memory_order_acquire); }
bool ReservedCounter::seed_total_after_quiescence(Amount value) noexcept {
    if (value>limit_) return false;
    Amount remaining=value;
    for (const auto& shard:shards_) {
        const Amount assigned=remaining<shard->reserve?remaining:shard->reserve;
        shard->used.store(assigned,std::memory_order_release); remaining-=assigned;
    }
    if (remaining != 0) return false;
    total_.store(value, std::memory_order_release);
    return true;
}
}  // namespace counter_poc
