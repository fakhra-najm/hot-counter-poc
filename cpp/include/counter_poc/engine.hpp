#pragma once
#include "counter_poc/hotkey_detector.hpp"
#include "counter_poc/reserved_counter.hpp"
#include "counter_poc/routing.hpp"
#include "counter_poc/strict_counter.hpp"
#include <memory>
namespace counter_poc {
class CounterEngine final {
public:
    CounterEngine(Amount limit, std::uint32_t shards, Amount danger_threshold, std::uint64_t hot_threshold_tps);
    Result apply(const Request& request) noexcept;
    void start_control_plane();
    bool peak_transition_requested() const noexcept { return peak_requested_.load(std::memory_order_acquire); }
    bool danger_transition_requested() const noexcept { return danger_requested_.load(std::memory_order_acquire); }
    // Both transitions require a completed admission drain by the external coordinator.
    bool enter_reserved_mode_after_quiescence() noexcept;
    bool enter_danger_mode_after_quiescence() noexcept;
    RoutingMode mode() const noexcept { return routing_.mode(); }
    Amount danger_threshold() const noexcept { return danger_threshold_; }
private:
    const Amount danger_threshold_;
    StrictCounter strict_;
    ReservedCounter reserved_;
    RoutingMap routing_;
    std::unique_ptr<HotKeyDetector> detector_;
    std::atomic<bool> peak_requested_{false};
    std::atomic<bool> danger_requested_{false};
};
}  // namespace counter_poc
