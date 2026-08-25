#pragma once
#include "counter_poc/types.hpp"
#include <vector>
namespace counter_poc {
// Replication datatype only. Mergeability is not financial-limit enforcement.
class GCounter final {
public:
    explicit GCounter(std::uint32_t components);
    void increment(std::uint32_t component, Amount delta);
    void merge(const GCounter& other);
    Amount total() const;
    const std::vector<Amount>& components() const noexcept { return components_; }
private: std::vector<Amount> components_;
};
}  // namespace counter_poc
