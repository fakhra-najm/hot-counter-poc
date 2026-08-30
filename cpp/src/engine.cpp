#include "counter_poc/engine.hpp"
#include <chrono>
namespace counter_poc {
static std::uint64_t monotonic_ns() noexcept {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}
CounterEngine::CounterEngine(Amount limit, std::uint32_t shards, Amount danger_threshold,
                             std::uint64_t hot_threshold_tps, std::uint32_t component_id)
    : CounterEngine({limit, shards, danger_threshold, hot_threshold_tps, component_id, 0}) {}

CounterEngine::CounterEngine(CounterEngineConfig config)
    : danger_threshold_(config.danger_threshold),
      local_peak_reservation_(config.local_peak_reservation == 0 ? config.limit
                                                                  : config.local_peak_reservation),
      component_id_(config.component_id), strict_(config.limit),
      reserved_(local_peak_reservation_, config.shards), routing_(config.shards),
      detector_(std::make_unique<HotKeyDetector>(
          config.hot_threshold_tps, [this](CounterId, std::uint64_t) {
              peak_requested_.store(true, std::memory_order_release);
          })) {}

CounterEngine::~CounterEngine() { stop_control_plane(); }

void CounterEngine::start_control_plane() {
    if (control_worker_.joinable()) return;
    stop_control_.store(false, std::memory_order_release);
    detector_->start();
    control_worker_ = std::thread(&CounterEngine::control_loop, this);
}

void CounterEngine::stop_control_plane() noexcept {
    stop_control_.store(true, std::memory_order_release);
    if (control_worker_.joinable()) control_worker_.join();
    detector_->stop();
}

void CounterEngine::set_replication_publisher(IReplicationPublisher* publisher) noexcept {
    replication_publisher_.store(publisher, std::memory_order_release);
}

void CounterEngine::set_limit_reached_publisher(ILimitReachedPublisher* publisher) noexcept {
    limit_reached_publisher_.store(publisher, std::memory_order_release);
}

Result CounterEngine::apply(const Request& request) noexcept {
    if (exhausted_cache_.known_exhausted()) return {Decision::Rejected, strict_.limit(), 0};
    if (!admission_gate_.try_enter()) return {Decision::Rejected, 0, 0};
    struct AdmissionGuard final {
        AdmissionGate& gate;
        ~AdmissionGuard() { gate.leave(); }
    } guard{admission_gate_};

    detector_->record(request.counter,monotonic_ns());
    const RoutingMode mode = routing_.mode();
    Result result{Decision::Rejected, 0, 0};
    switch (mode) {
        case RoutingMode::Strict:
            result = strict_.apply(request.delta);
            break;
        case RoutingMode::ReservedPeak: {
            result = reserved_.apply(routing_.shard_for(request.route_hash),request.delta);
            break;
        }
        case RoutingMode::DangerStrict:
            result = strict_.apply(request.delta);
            break;
        case RoutingMode::RemoteStrict:
            result = {Decision::Moved, strict_.value(), 0};
            break;
    }
    if (result.decision == Decision::Accepted) {
        remember_counter(request.counter);
        if (mode != RoutingMode::ReservedPeak && result.observed == strict_.limit()) {
            publish_limit_reached(request.counter);
        } else if (mode == RoutingMode::ReservedPeak && local_peak_reservation_ == strict_.limit() &&
                   reserved_.mark_exhaustion_if_new(result.shard) &&
                   exhausted_peak_shards_.fetch_add(1, std::memory_order_acq_rel) + 1 ==
                       reserved_.shards()) {
            publish_limit_reached(request.counter);
        }
    }
    return result;
}

bool CounterEngine::enter_reserved_mode_after_quiescence() noexcept {
    return transition_to_reserved();
}

bool CounterEngine::enter_danger_mode_after_quiescence() noexcept {
    return transition_to_danger();
}

bool CounterEngine::begin_distributed_handoff(DrainedPeakState& state) noexcept {
    bool expected = false;
    if (!distributed_handoff_active_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                              std::memory_order_acquire))
        return false;
    if (routing_.mode() != RoutingMode::ReservedPeak) {
        distributed_handoff_active_.store(false, std::memory_order_release);
        return false;
    }
    close_and_drain();
    state = {active_counter_.load(std::memory_order_acquire),
             replication_epoch_.load(std::memory_order_acquire), reserved_.total()};
    return true;
}

void CounterEngine::abort_distributed_handoff() noexcept {
    if (!distributed_handoff_active_.exchange(false, std::memory_order_acq_rel)) return;
    if (routing_.mode() == RoutingMode::ReservedPeak) admission_gate_.open();
}

bool CounterEngine::commit_distributed_handoff(Amount global_total, bool strict_owner) noexcept {
    if (!distributed_handoff_active_.load(std::memory_order_acquire) ||
        routing_.mode() != RoutingMode::ReservedPeak || global_total > strict_.limit())
        return false;
    if (!strict_.seed_after_quiescence(global_total)) return false;
    routing_.publish(strict_owner ? RoutingMode::DangerStrict : RoutingMode::RemoteStrict);
    exhausted_peak_shards_.store(0, std::memory_order_release);
    replication_epoch_.fetch_add(1, std::memory_order_acq_rel);
    danger_requested_.store(false, std::memory_order_release);
    distributed_handoff_active_.store(false, std::memory_order_release);
    if (global_total == strict_.limit()) {
        if (strict_owner) {
            publish_limit_reached(active_counter_.load(std::memory_order_acquire));
        } else {
            exhausted_cache_.mark_exhausted();
        }
    }
    admission_gate_.open();
    return true;
}

bool CounterEngine::recover_committed_handoff(const DrainedPeakState& state, Amount global_total,
                                              bool strict_owner) noexcept {
    if (global_total > strict_.limit()) return false;
    close_and_drain();
    if (!strict_.seed_after_quiescence(global_total)) return false;
    active_counter_.store(state.counter, std::memory_order_relaxed);
    has_active_counter_.store(true, std::memory_order_release);
    // The next snapshot must not look older than the handoff that selected
    // the strict owner. A wrap is not operationally realistic, but retaining
    // the recorded epoch is still safer than silently resetting to zero.
    replication_epoch_.store(state.epoch, std::memory_order_release);
    routing_.publish(strict_owner ? RoutingMode::DangerStrict : RoutingMode::RemoteStrict);
    exhausted_peak_shards_.store(0, std::memory_order_release);
    distributed_handoff_active_.store(false, std::memory_order_release);
    if (global_total == strict_.limit()) {
        if (strict_owner) {
            publish_limit_reached(state.counter);
        } else {
            exhausted_cache_.mark_exhausted();
        }
    }
    admission_gate_.open();
    return true;
}

void CounterEngine::fence_after_interrupted_handoff() noexcept { close_and_drain(); }

bool CounterEngine::close_and_drain() noexcept {
    admission_gate_.close();
    while (!admission_gate_.drained()) std::this_thread::yield();
    return true;
}

bool CounterEngine::transition_to_reserved() noexcept {
    if (routing_.mode()!=RoutingMode::Strict) return false;
    close_and_drain();
    if (strict_.value() > local_peak_reservation_ ||
        !reserved_.seed_total_after_quiescence(strict_.value())) {
        admission_gate_.open();
        return false;
    }
    routing_.publish(RoutingMode::ReservedPeak);
    exhausted_peak_shards_.store(reserved_.exhausted_shards(), std::memory_order_release);
    replication_epoch_.fetch_add(1, std::memory_order_acq_rel);
    peak_requested_.store(false,std::memory_order_release);
    danger_requested_.store(false,std::memory_order_release);
    admission_gate_.open();
    return true;
}

bool CounterEngine::transition_to_danger() noexcept {
    if (routing_.mode()!=RoutingMode::ReservedPeak) return false;
    // A process with only a component reservation cannot reconstruct the
    // global strict value without a distributed barrier and component
    // snapshot collection. Refuse an unsafe local collapse.
    if (local_peak_reservation_ != strict_.limit() ||
        distributed_handoff_active_.load(std::memory_order_acquire))
        return false;
    close_and_drain();
    if (!strict_.seed_after_quiescence(reserved_.total())) {
        admission_gate_.open();
        return false;
    }
    routing_.publish(RoutingMode::DangerStrict);
    exhausted_peak_shards_.store(0, std::memory_order_release);
    replication_epoch_.fetch_add(1, std::memory_order_acq_rel);
    danger_requested_.store(false, std::memory_order_release);
    admission_gate_.open();
    return true;
}

void CounterEngine::control_loop() noexcept {
    while (!stop_control_.load(std::memory_order_acquire)) {
        const RoutingMode mode = routing_.mode();
        const bool has_counter = has_active_counter_.load(std::memory_order_acquire);
        const CounterId counter = active_counter_.load(std::memory_order_acquire);
        Amount component_value = 0;
        const bool replicate = mode != RoutingMode::RemoteStrict;
        if (mode == RoutingMode::ReservedPeak) {
            component_value = reserved_.total();
            if (local_peak_reservation_ == strict_.limit() &&
                (danger_threshold_ >= strict_.limit() ||
                 strict_.limit() - component_value <= danger_threshold_)) {
                danger_requested_.store(true, std::memory_order_release);
            }
        } else if (mode != RoutingMode::RemoteStrict) {
            component_value = strict_.value();
        }
        const std::uint64_t epoch = replication_epoch_.load(std::memory_order_acquire);
        if (replicate && has_counter && (component_value != last_replicated_value_ ||
                            epoch != last_replicated_epoch_)) {
            publish_snapshot(counter, component_value);
            last_replicated_value_ = component_value;
            last_replicated_epoch_ = epoch;
        }

        if (mode == RoutingMode::Strict && peak_requested_.load(std::memory_order_acquire)) {
            transition_to_reserved();
        } else if (mode == RoutingMode::ReservedPeak &&
                   danger_requested_.load(std::memory_order_acquire)) {
            transition_to_danger();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void CounterEngine::remember_counter(CounterId counter) noexcept {
    if (has_active_counter_.load(std::memory_order_acquire) &&
        active_counter_.load(std::memory_order_relaxed) == counter)
        return;
    active_counter_.store(counter, std::memory_order_relaxed);
    has_active_counter_.store(true, std::memory_order_release);
}

void CounterEngine::publish_snapshot(CounterId counter, Amount component_value) noexcept {
    IReplicationPublisher* const publisher = replication_publisher_.load(std::memory_order_acquire);
    if (publisher == nullptr) return;
    publisher->publish(
        {counter, replication_epoch_.load(std::memory_order_acquire), component_id_, component_value});
}

void CounterEngine::publish_limit_reached(CounterId counter) noexcept {
    if (!exhausted_cache_.mark_exhausted()) return;
    ILimitReachedPublisher* const publisher =
        limit_reached_publisher_.load(std::memory_order_acquire);
    if (publisher != nullptr) {
        publisher->publish(
            {counter, strict_.limit(), replication_epoch_.load(std::memory_order_acquire)});
    }
}
}  // namespace counter_poc
