#include "counter_poc/reservation_plan.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace counter_poc {

ReservationPlan::ReservationPlan(Amount global_limit,
                                 std::vector<ComponentReservation> components)
    : global_limit_(global_limit), allocated_total_(0), components_(std::move(components)) {
    std::sort(components_.begin(), components_.end(),
              [](const ComponentReservation& left, const ComponentReservation& right) {
                  return left.component_id < right.component_id;
              });

    for (std::size_t index = 0; index < components_.size(); ++index) {
        if (index != 0 && components_[index - 1].component_id == components_[index].component_id)
            throw std::invalid_argument("duplicate reservation component");
        if (components_[index].capacity > std::numeric_limits<Amount>::max() - allocated_total_)
            throw std::overflow_error("reservation total overflow");
        allocated_total_ += components_[index].capacity;
    }
    if (allocated_total_ > global_limit_)
        throw std::invalid_argument("reservation plan exceeds the global limit");
}

Amount ReservationPlan::capacity_for(std::uint32_t component_id) const noexcept {
    const auto found = std::lower_bound(
        components_.begin(), components_.end(), component_id,
        [](const ComponentReservation& component, std::uint32_t id) {
            return component.component_id < id;
        });
    return found != components_.end() && found->component_id == component_id ? found->capacity : 0;
}

bool ReservationPlan::has_component(std::uint32_t component_id) const noexcept {
    const auto found = std::lower_bound(
        components_.begin(), components_.end(), component_id,
        [](const ComponentReservation& component, std::uint32_t id) {
            return component.component_id < id;
        });
    return found != components_.end() && found->component_id == component_id;
}

}  // namespace counter_poc
