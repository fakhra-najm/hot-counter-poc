#pragma once

#include "counter_poc/runtime_config.hpp"

#include <memory>

namespace counter_poc {

// The product boundary shared by every data-plane adapter. It owns one engine
// and its optional asynchronous control-plane services; transports only turn
// requests into CounterEngine::apply calls and serialize Result values.
class CounterRuntime final {
public:
    explicit CounterRuntime(CounterRuntimeConfig config);
    ~CounterRuntime();

    CounterRuntime(const CounterRuntime&) = delete;
    CounterRuntime& operator=(const CounterRuntime&) = delete;

    void start();
    void stop() noexcept;
    bool started() const noexcept { return started_; }

    CounterEngine& engine() noexcept { return engine_; }
    const CounterEngine& engine() const noexcept { return engine_; }
    std::uint16_t replication_port() const noexcept;
    std::uint16_t handoff_port() const noexcept;

private:
    CounterRuntimeConfig config_;
    CounterEngine engine_;
    std::unique_ptr<ReliableUdpReplicator> replication_;
    std::unique_ptr<DistributedHandoffController> handoff_;
    bool started_{false};
};

}  // namespace counter_poc
