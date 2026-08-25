#pragma once
#include "counter_poc/types.hpp"
#include <atomic>
#include <memory>
#include <vector>
namespace counter_poc {
// Escrow-style capacity reservation: never overshoots, but can false-reject
// when the selected shard's reserve is empty while another shard has capacity.
class ReservedCounter final {
public:
    ReservedCounter(Amount limit, std::uint32_t shards);
    Result apply(std::uint32_t shard, Amount delta) noexcept;
    Amount total() const noexcept;
    // Control-plane only: installs an exact prior logical value after admission drain.
    bool seed_total_after_quiescence(Amount value) noexcept;
    Amount limit() const noexcept { return limit_; }
    std::uint32_t shards() const noexcept { return static_cast<std::uint32_t>(shards_.size()); }
private:
    struct alignas(64) Shard { std::atomic<Amount> used{0}; Amount reserve{0}; };
    Amount limit_;
    std::vector<std::unique_ptr<Shard>> shards_;
    alignas(64) std::atomic<Amount> total_{0};
};
}  // namespace counter_poc
