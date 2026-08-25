#include "counter_poc/gcounter.hpp"
#include <algorithm>
#include <limits>
#include <stdexcept>
namespace counter_poc {
GCounter::GCounter(std::uint32_t components) : components_(components,0) {}
void GCounter::increment(std::uint32_t component, Amount delta) {
    if (component >= components_.size()) throw std::out_of_range("invalid G-Counter component");
    if (delta > std::numeric_limits<Amount>::max() - components_[component])
        throw std::overflow_error("G-Counter component overflow");
    components_[component] += delta;
}
void GCounter::merge(const GCounter& other) {
    if (components_.size() != other.components_.size())
        throw std::invalid_argument("G-Counter component counts differ");
    for (std::size_t i = 0; i < components_.size(); ++i)
        components_[i] = std::max(components_[i], other.components_[i]);
}
Amount GCounter::total() const {
    Amount total = 0;
    for (Amount value : components_) {
        if (value > std::numeric_limits<Amount>::max() - total)
            throw std::overflow_error("G-Counter total overflow");
        total += value;
    }
    return total;
}
}  // namespace counter_poc
