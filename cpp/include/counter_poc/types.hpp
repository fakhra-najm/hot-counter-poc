#pragma once
#include <cstdint>
namespace counter_poc {
using CounterId = std::uint64_t;
using Amount = std::uint64_t;
enum class Decision : std::uint8_t { Accepted, Rejected, Moved };
enum class RoutingMode : std::uint8_t { Strict, ReservedPeak, DangerStrict, RemoteStrict };
struct Request { CounterId counter; Amount delta; std::uint64_t route_hash{0}; };
struct Result { Decision decision; Amount observed; std::uint32_t shard; };
// Monotonic snapshot for replication. A receiver keeps the largest value for
// each (counter, epoch, component) tuple, making duplicate UDP packets safe.
struct ReplicationUpdate {
    CounterId counter;
    std::uint64_t epoch;
    std::uint32_t component;
    Amount value;
};

class IReplicationPublisher {
public:
    virtual ~IReplicationPublisher() = default;
    virtual void publish(const ReplicationUpdate& update) noexcept = 0;
};

// This event is emitted only by the successful operation that reaches the
// limit exactly. It is intentionally separate from replication: consumers
// may cache only this monotonic, definitive state at an edge.
struct LimitReachedEvent {
    CounterId counter;
    Amount limit;
    std::uint64_t epoch;
};

class ILimitReachedPublisher {
public:
    virtual ~ILimitReachedPublisher() = default;
    virtual void publish(const LimitReachedEvent& event) noexcept = 0;
};
}  // namespace counter_poc
