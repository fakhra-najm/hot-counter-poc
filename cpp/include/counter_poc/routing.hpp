#pragma once
#include "counter_poc/types.hpp"
#include <atomic>
namespace counter_poc {
class RoutingMap final {
public:
    explicit RoutingMap(std::uint32_t shards) noexcept : shards_(shards) {}
    RoutingMode mode() const noexcept { return mode_.load(std::memory_order_acquire); }
    void publish(RoutingMode next) noexcept { mode_.store(next, std::memory_order_release); }
    std::uint32_t shard_for(CounterId id) const noexcept;
private:
    std::atomic<RoutingMode> mode_{RoutingMode::Strict};
    const std::uint32_t shards_;
};
}  // namespace counter_poc
