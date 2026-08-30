#pragma once

#include "counter_poc/engine.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace counter_poc {

struct DistributedPeer {
    std::uint32_t component_id;
    std::string host;
    std::uint16_t port;
};

struct DistributedHandoffConfig {
    std::uint32_t component_id;
    std::uint32_t leader_component_id;
    std::uint32_t strict_owner_component_id;
    std::uint16_t bind_port;
    std::string bind_host{"0.0.0.0"};
    // Complete fixed membership, including this process. It must match the
    // escrow allocation used to start every peak component.
    std::vector<std::uint32_t> members;
    // One endpoint for every other member.
    std::vector<DistributedPeer> peers;
    std::chrono::milliseconds retry_after{10};
    std::chrono::milliseconds prepare_timeout{1000};
};

struct DistributedHandoffStats {
    std::uint64_t triggers{};
    std::uint64_t prepares{};
    std::uint64_t commits{};
    std::uint64_t aborts{};
};

// A fixed-membership two-phase handoff. Prepare closes and drains every peak
// component. Commit installs the summed value on exactly one strict owner and
// changes every other component to RemoteStrict, where it rejects requests.
// A timeout before commit aborts safely; after commit the controller retries
// forever rather than reopening a potentially split cluster.
class DistributedHandoffController final {
public:
    DistributedHandoffController(CounterEngine& engine, DistributedHandoffConfig config);
    ~DistributedHandoffController();

    DistributedHandoffController(const DistributedHandoffController&) = delete;
    DistributedHandoffController& operator=(const DistributedHandoffController&) = delete;

    void start();
    void stop() noexcept;
    DistributedHandoffStats stats() const noexcept;
    std::uint16_t bound_port() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace counter_poc
