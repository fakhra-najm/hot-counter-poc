#include "counter_poc/engine.hpp"
#include <chrono>
namespace counter_poc {
static std::uint64_t monotonic_ns() noexcept {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}
CounterEngine::CounterEngine(Amount limit, std::uint32_t shards, Amount danger_threshold, std::uint64_t hot_threshold_tps)
    : danger_threshold_(danger_threshold), strict_(limit), reserved_(limit,shards), routing_(shards),
      detector_(std::make_unique<HotKeyDetector>(hot_threshold_tps,[this](CounterId,std::uint64_t){ peak_requested_.store(true,std::memory_order_release); })) {}
void CounterEngine::start_control_plane() { detector_->start(); }
Result CounterEngine::apply(const Request& request) noexcept {
    detector_->record(request.counter,monotonic_ns());
    switch (routing_.mode()) {
        case RoutingMode::Strict: return strict_.apply(request.delta);
        case RoutingMode::ReservedPeak: {
            const Result result = reserved_.apply(routing_.shard_for(request.route_hash),request.delta);
            if (result.decision == Decision::Accepted) {
                const Amount total = reserved_.total();
                if (danger_threshold_ >= reserved_.limit() || reserved_.limit() - total <= danger_threshold_)
                    danger_requested_.store(true, std::memory_order_release);
            }
            return result;
        }
        case RoutingMode::DangerStrict: return strict_.apply(request.delta);
    }
    return {Decision::Rejected,0,0};
}
bool CounterEngine::enter_reserved_mode_after_quiescence() noexcept {
    if (routing_.mode()!=RoutingMode::Strict) return false;
    if (!reserved_.seed_total_after_quiescence(strict_.value())) return false;
    routing_.publish(RoutingMode::ReservedPeak);
    peak_requested_.store(false,std::memory_order_release);
    danger_requested_.store(false,std::memory_order_release);
    return true;
}
bool CounterEngine::enter_danger_mode_after_quiescence() noexcept {
    if (routing_.mode()!=RoutingMode::ReservedPeak) return false;
    if (!strict_.seed_after_quiescence(reserved_.total())) return false;
    routing_.publish(RoutingMode::DangerStrict);
    danger_requested_.store(false, std::memory_order_release);
    return true;
}
}  // namespace counter_poc
