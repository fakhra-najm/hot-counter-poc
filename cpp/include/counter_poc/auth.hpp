#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace counter_poc {

// Truncated HMAC-SHA-256 for fixed control frames. The 128-bit tag is enough
// to reject unauthenticated UDP control traffic without adding a crypto
// dependency to the hot data path.
class ControlAuthenticator final {
public:
    static constexpr std::size_t kTagSize = 16;
    using Tag = std::array<std::uint8_t, kTagSize>;

    static Tag sign(std::span<const std::uint8_t> key,
                    std::span<const std::uint8_t> message) noexcept;
    static bool verify(std::span<const std::uint8_t> key,
                       std::span<const std::uint8_t> message,
                       const Tag& tag) noexcept;
};

}  // namespace counter_poc
