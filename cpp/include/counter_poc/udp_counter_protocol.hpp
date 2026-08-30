#pragma once

#include "counter_poc/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace counter_poc {

// Fixed-size, allocation-free UDP payload used by the DPDK adapter and its
// smoke client. Ethernet/IP/UDP headers remain owned by the DPDK adapter.
class UdpCounterProtocol final {
public:
    static constexpr std::size_t kFrameSize = 40;

    struct Request {
        std::uint64_t request_id{};
        Amount delta{};
        CounterId counter{};
    };

    struct Reply {
        std::uint64_t request_id{};
        Result result{};
        CounterId counter{};
        // Host-order IPv4 and UDP port used only for a MOVED reply. Zeros
        // mean that the UDP client/router must use its configured owner map.
        std::uint32_t strict_owner_ipv4{};
        std::uint16_t strict_owner_port{};
    };

    static std::array<std::uint8_t, kFrameSize> encode_request(const Request& request) noexcept;
    static bool decode_request(std::span<const std::uint8_t> bytes, Request& request) noexcept;
    static std::array<std::uint8_t, kFrameSize> encode_reply(const Reply& reply) noexcept;
    static bool decode_reply(std::span<const std::uint8_t> bytes, Reply& reply) noexcept;
};

}  // namespace counter_poc
