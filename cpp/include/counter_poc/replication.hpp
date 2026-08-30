#pragma once

#include "counter_poc/mpmc_queue.hpp"
#include "counter_poc/types.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace counter_poc {

struct UdpPeer {
    std::string host;
    std::uint16_t port;
};

struct UdpReplicationConfig {
    std::uint32_t component_id{0};
    std::uint16_t bind_port{0};
    std::vector<UdpPeer> peers;
    std::chrono::milliseconds retry_after{10};
    std::uint32_t max_retries{8};
    std::size_t max_pending{65536};
    // Use 0.0.0.0 for a node that receives from the network. Tests can bind
    // only to loopback, keeping them isolated from external interfaces.
    std::string bind_host{"0.0.0.0"};
};

struct ReplicationStats {
    std::uint64_t queued{};
    std::uint64_t dropped{};
    std::uint64_t sent{};
    std::uint64_t acknowledged{};
    std::uint64_t retried{};
    std::uint64_t received{};
};

// UDP replication is asynchronous and does not participate in admission.
// Each snapshot is cumulative, so a retry, duplicate, or reordering cannot
// make the receiver count a component twice. ACKs bound retry work.
class ReliableUdpReplicator final : public IReplicationPublisher {
public:
    explicit ReliableUdpReplicator(UdpReplicationConfig config);
    ~ReliableUdpReplicator() override;

    ReliableUdpReplicator(const ReliableUdpReplicator&) = delete;
    ReliableUdpReplicator& operator=(const ReliableUdpReplicator&) = delete;

    void start();
    void stop() noexcept;
    void publish(const ReplicationUpdate& update) noexcept override;
    Amount component_value(CounterId counter, std::uint64_t epoch,
                           std::uint32_t component) const noexcept;
    // Sum of the largest snapshot seen for every component in an epoch. This
    // is observational CRDT state, not an admission authority.
    Amount merged_total(CounterId counter, std::uint64_t epoch) const noexcept;
    ReplicationStats stats() const noexcept;
    std::uint16_t bound_port() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace counter_poc
