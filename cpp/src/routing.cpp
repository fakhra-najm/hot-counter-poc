#include "counter_poc/routing.hpp"
namespace counter_poc {
std::uint32_t RoutingMap::shard_for(CounterId id) const noexcept {
    // Fibonacci hashing avoids a power-of-two low-bit bias for sequential counter IDs.
    return static_cast<std::uint32_t>((id*11400714819323198485ULL)>>32)%shards_;
}
}  // namespace counter_poc
