#pragma once

#include "counter_poc/types.hpp"

#include <cstdint>
#include <vector>

namespace counter_poc {

struct ComponentReservation {
    std::uint32_t component_id;
    Amount capacity;
};

// Immutable cluster-wide escrow allocation. A valid plan makes every peak
// node safe independently: no two components can spend the same capacity.
// It deliberately permits unused capacity, which is safer than overbooking.
class ReservationPlan final {
public:
    ReservationPlan(Amount global_limit, std::vector<ComponentReservation> components);

    Amount global_limit() const noexcept { return global_limit_; }
    Amount allocated_total() const noexcept { return allocated_total_; }
    Amount capacity_for(std::uint32_t component_id) const noexcept;
    bool has_component(std::uint32_t component_id) const noexcept;

private:
    Amount global_limit_;
    Amount allocated_total_;
    std::vector<ComponentReservation> components_;
};

}  // namespace counter_poc
