#pragma once
#include "counter_poc/counter.hpp"
#include <atomic>
namespace counter_poc {
class StrictCounter final : public ICounter {
public:
    explicit StrictCounter(Amount limit, Amount initial = 0) noexcept;
    Result apply(Amount delta) noexcept override;
    Amount value() const noexcept override;
    Amount limit() const noexcept override { return limit_; }
    // Control-plane only: callers must first drain all old-mode requests.
    [[nodiscard]] bool seed_after_quiescence(Amount value) noexcept;
private:
    alignas(64) std::atomic<Amount> value_;
    const Amount limit_;
};
}  // namespace counter_poc
