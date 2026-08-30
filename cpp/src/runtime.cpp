#include "counter_poc/runtime.hpp"

#include <stdexcept>
#include <utility>

namespace counter_poc {

CounterRuntime::CounterRuntime(CounterRuntimeConfig config)
    : config_(std::move(config)), engine_(config_.engine) {}

CounterRuntime::~CounterRuntime() { stop(); }

void CounterRuntime::start() {
    if (started_) return;
    try {
        if (config_.start_peak && !engine_.enter_reserved_mode_after_quiescence())
            throw std::runtime_error("cannot enter initial peak mode");

        if (config_.replication.has_value()) {
            replication_ = std::make_unique<ReliableUdpReplicator>(*config_.replication);
            replication_->start();
            engine_.set_replication_publisher(replication_.get());
        }
        if (config_.handoff.has_value()) {
            handoff_ = std::make_unique<DistributedHandoffController>(engine_, *config_.handoff);
            handoff_->start();
        }
        engine_.start_control_plane();
        started_ = true;
    } catch (...) {
        engine_.stop_control_plane();
        handoff_.reset();
        engine_.set_replication_publisher(nullptr);
        replication_.reset();
        throw;
    }
}

void CounterRuntime::stop() noexcept {
    if (!started_ && replication_ == nullptr && handoff_ == nullptr) return;
    engine_.stop_control_plane();
    handoff_.reset();
    engine_.set_replication_publisher(nullptr);
    replication_.reset();
    started_ = false;
}

std::uint16_t CounterRuntime::replication_port() const noexcept {
    return replication_ == nullptr ? 0 : replication_->bound_port();
}

std::uint16_t CounterRuntime::handoff_port() const noexcept {
    return handoff_ == nullptr ? 0 : handoff_->bound_port();
}

}  // namespace counter_poc
