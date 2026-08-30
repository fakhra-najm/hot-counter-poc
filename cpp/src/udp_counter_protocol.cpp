#include "counter_poc/udp_counter_protocol.hpp"

namespace counter_poc {
namespace {

constexpr std::uint32_t kMagic = 0x48435031U;  // HCP1
constexpr std::uint8_t kVersion = 1;
constexpr std::uint8_t kRequest = 1;
constexpr std::uint8_t kReply = 2;

void write_u16(std::uint8_t* bytes, std::size_t offset, std::uint16_t value) noexcept {
    bytes[offset] = static_cast<std::uint8_t>(value >> 8U);
    bytes[offset + 1] = static_cast<std::uint8_t>(value);
}

void write_u32(std::uint8_t* bytes, std::size_t offset, std::uint32_t value) noexcept {
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        const std::size_t shift = (sizeof(value) - 1U - index) * 8U;
        bytes[offset + index] = static_cast<std::uint8_t>(value >> shift);
    }
}

void write_u64(std::uint8_t* bytes, std::size_t offset, std::uint64_t value) noexcept {
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        const std::size_t shift = (sizeof(value) - 1U - index) * 8U;
        bytes[offset + index] = static_cast<std::uint8_t>(value >> shift);
    }
}

std::uint16_t read_u16(const std::uint8_t* bytes, std::size_t offset) noexcept {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) |
                                      bytes[offset + 1]);
}

std::uint32_t read_u32(const std::uint8_t* bytes, std::size_t offset) noexcept {
    std::uint32_t value = 0;
    for (std::size_t index = 0; index < sizeof(value); ++index)
        value = (value << 8U) | bytes[offset + index];
    return value;
}

std::uint64_t read_u64(const std::uint8_t* bytes, std::size_t offset) noexcept {
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < sizeof(value); ++index)
        value = (value << 8U) | bytes[offset + index];
    return value;
}

bool valid_prefix(std::span<const std::uint8_t> bytes, std::uint8_t type) noexcept {
    return bytes.size() == UdpCounterProtocol::kFrameSize && read_u32(bytes.data(), 0) == kMagic &&
           bytes[4] == kVersion && bytes[5] == type;
}

}  // namespace

std::array<std::uint8_t, UdpCounterProtocol::kFrameSize>
UdpCounterProtocol::encode_request(const Request& request) noexcept {
    std::array<std::uint8_t, kFrameSize> bytes{};
    write_u32(bytes.data(), 0, kMagic);
    bytes[4] = kVersion;
    bytes[5] = kRequest;
    write_u64(bytes.data(), 8, request.request_id);
    write_u64(bytes.data(), 16, request.delta);
    write_u64(bytes.data(), 24, request.counter);
    return bytes;
}

bool UdpCounterProtocol::decode_request(std::span<const std::uint8_t> bytes,
                                        Request& request) noexcept {
    if (!valid_prefix(bytes, kRequest)) return false;
    request = {read_u64(bytes.data(), 8), read_u64(bytes.data(), 16), read_u64(bytes.data(), 24)};
    return true;
}

std::array<std::uint8_t, UdpCounterProtocol::kFrameSize>
UdpCounterProtocol::encode_reply(const Reply& reply) noexcept {
    std::array<std::uint8_t, kFrameSize> bytes{};
    write_u32(bytes.data(), 0, kMagic);
    bytes[4] = kVersion;
    bytes[5] = kReply;
    bytes[6] = reply.result.decision == Decision::Accepted
                   ? 1U
                   : reply.result.decision == Decision::Moved ? 2U : 0U;
    write_u64(bytes.data(), 8, reply.request_id);
    write_u64(bytes.data(), 16, reply.result.observed);
    write_u64(bytes.data(), 24, reply.counter);
    if (reply.result.decision == Decision::Moved) {
        write_u32(bytes.data(), 32, reply.strict_owner_ipv4);
        write_u16(bytes.data(), 36, reply.strict_owner_port);
    }
    return bytes;
}

bool UdpCounterProtocol::decode_reply(std::span<const std::uint8_t> bytes, Reply& reply) noexcept {
    if (!valid_prefix(bytes, kReply) || bytes[6] > 2U) return false;
    const Decision decision = bytes[6] == 1U ? Decision::Accepted :
                              bytes[6] == 2U ? Decision::Moved : Decision::Rejected;
    reply = {read_u64(bytes.data(), 8),
             {decision, read_u64(bytes.data(), 16), 0},
             read_u64(bytes.data(), 24),
             read_u32(bytes.data(), 32),
             read_u16(bytes.data(), 36)};
    return true;
}

}  // namespace counter_poc
