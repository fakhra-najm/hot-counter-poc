#pragma once
#include "counter_poc/admission_gate.hpp"
#include "counter_poc/hotkey_detector.hpp"
#include "counter_poc/limit_reached.hpp"
#include "counter_poc/reserved_counter.hpp"
#include "counter_poc/routing.hpp"
#include "counter_poc/strict_counter.hpp"
#include <limits>
#include <memory>
#include <thread>
namespace counter_poc {
struct DrainedPeakState {
    CounterId counter{};
    std::uint64_t epoch{};
    Amount component_total{};
};

struct CounterEngineConfig {
    Amount limit;
    std::uint32_t shards;
    Amount danger_threshold;
    std::uint64_t hot_threshold_tps;
    std::uint32_t component_id{0};
    // Zero means this process owns the entire limit in peak mode. A smaller
    // value is an externally validated cluster escrow allocation.
    Amount local_peak_reservation{0};
};

class CounterEngine final {
public:
    CounterEngine(Amount limit, std::uint32_t shards, Amount danger_threshold,
                  std::uint64_t hot_threshold_tps, std::uint32_t component_id = 0);
    explicit CounterEngine(CounterEngineConfig config);
    ~CounterEngine();
    CounterEngine(const CounterEngine&) = delete;
    CounterEngine& operator=(const CounterEngine&) = delete;

    Result apply(const Request& request) noexcept;
    void start_control_plane();
    void stop_control_plane() noexcept;
    void set_replication_publisher(IReplicationPublisher* publisher) noexcept;
    void set_limit_reached_publisher(ILimitReachedPublisher* publisher) noexcept;
    bool peak_transition_requested() const noexcept { return peak_requested_.load(std::memory_order_acquire); }
    bool danger_transition_requested() const noexcept { return danger_requested_.load(std::memory_order_acquire); }
    // Both transitions close the local admission gate and wait for drain.
    // A component-reservation node refuses the unsafe peak-to-strict collapse.
    bool enter_reserved_mode_after_quiescence() noexcept;
    bool enter_danger_mode_after_quiescence() noexcept;
    RoutingMode mode() const noexcept { return routing_.mode(); }
    Amount danger_threshold() const noexcept { return danger_threshold_; }
    Amount local_peak_reservation() const noexcept { return local_peak_reservation_; }
    Amount limit() const noexcept { return strict_.limit(); }
    Amount current_value() const noexcept {
        return routing_.mode() == RoutingMode::ReservedPeak ? reserved_.total() : strict_.value();
    }
    Amount current_peak_total() const noexcept { return reserved_.total(); }
    std::uint64_t replication_epoch() const noexcept { return replication_epoch_.load(std::memory_order_acquire); }
    bool limit_exhausted() const noexcept { return exhausted_cache_.known_exhausted(); }
    // Distributed control-plane API. begin_* closes local admissions and
    // returns an exact component total only after every admitted peak request
    // has finished. The caller must either commit or abort that handoff.
    bool begin_distributed_handoff(DrainedPeakState& state) noexcept;
    void abort_distributed_handoff() noexcept;
    bool commit_distributed_handoff(Amount global_total, bool strict_owner) noexcept;
    // Recovery API for a committed record written before the mode change. It
    // is intentionally separate from commit_distributed_handoff(): recovery
    // happens before the daemon starts accepting client connections.
    bool recover_committed_handoff(const DrainedPeakState& state, Amount global_total,
                                   bool strict_owner) noexcept;
    // A prepared handoff without a durable decision is ambiguous after a
    // crash. Keep the admission gate closed until an operator resolves it.
    void fence_after_interrupted_handoff() noexcept;
private:
    bool transition_to_reserved() noexcept;
    bool transition_to_danger() noexcept;
    bool close_and_drain() noexcept;
    void control_loop() noexcept;
    void remember_counter(CounterId counter) noexcept;
    void publish_snapshot(CounterId counter, Amount component_value) noexcept;
    void publish_limit_reached(CounterId counter) noexcept;

    const Amount danger_threshold_;
    const Amount local_peak_reservation_;
    const std::uint32_t component_id_;
    StrictCounter strict_;
    ReservedCounter reserved_;
    RoutingMap routing_;
    std::unique_ptr<HotKeyDetector> detector_;
    AdmissionGate admission_gate_;
    std::atomic<bool> peak_requested_{false};
    std::atomic<bool> danger_requested_{false};
    std::atomic<bool> distributed_handoff_active_{false};
    std::atomic<std::uint32_t> exhausted_peak_shards_{0};
    std::atomic<IReplicationPublisher*> replication_publisher_{nullptr};
    std::atomic<ILimitReachedPublisher*> limit_reached_publisher_{nullptr};
    LimitReachedCache exhausted_cache_;
    std::atomic<CounterId> active_counter_{0};
    std::atomic<bool> has_active_counter_{false};
    std::atomic<std::uint64_t> replication_epoch_{0};
    Amount last_replicated_value_{0};
    std::uint64_t last_replicated_epoch_{std::numeric_limits<std::uint64_t>::max()};
    std::atomic<bool> stop_control_{false};
    std::thread control_worker_;
};
}  // namespace counter_poc
