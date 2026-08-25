#pragma once
#include <cstdint>
namespace counter_poc {
using CounterId = std::uint64_t;
using Amount = std::uint64_t;
enum class Decision : std::uint8_t { Accepted, Rejected };
enum class RoutingMode : std::uint8_t { Strict, ReservedPeak, DangerStrict };
struct Request { CounterId counter; Amount delta; std::uint64_t route_hash{0}; };
struct Result { Decision decision; Amount observed; std::uint32_t shard; };
}  // namespace counter_poc
