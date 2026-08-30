// Minimal kernel-UDP client for exercising the DPDK server's 32-byte request
// protocol. It is not a benchmark; use it first to verify ARP and replies.
#include <arpa/inet.h>
#include <charconv>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr std::uint32_t kWireMagic = 0x48435031U;
constexpr std::uint8_t kWireVersion = 1;
constexpr std::uint8_t kWireRequest = 1;
constexpr std::uint8_t kWireAck = 2;

void write_u32(std::uint8_t* bytes, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < sizeof(value); ++index)
        bytes[offset + index] = static_cast<std::uint8_t>(value >> ((3U - index) * 8U));
}
void write_u64(std::uint8_t* bytes, std::size_t offset, std::uint64_t value) {
    for (std::size_t index = 0; index < sizeof(value); ++index)
        bytes[offset + index] = static_cast<std::uint8_t>(value >> ((7U - index) * 8U));
}
std::uint32_t read_u32(const std::uint8_t* bytes, std::size_t offset) {
    return (static_cast<std::uint32_t>(bytes[offset]) << 24U) |
           (static_cast<std::uint32_t>(bytes[offset + 1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[offset + 2]) << 8U) |
           static_cast<std::uint32_t>(bytes[offset + 3]);
}
std::uint64_t read_u64(const std::uint8_t* bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < sizeof(value); ++index) value = (value << 8U) | bytes[offset + index];
    return value;
}
template <typename Number>
bool parse(std::string_view value, Number& out) {
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), out);
    return error == std::errc{} && end == value.data() + value.size();
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: counterd_udp_client IPv4 port counter delta\n";
        return 2;
    }
    std::uint16_t port = 0;
    std::uint64_t counter = 0;
    std::uint64_t delta = 0;
    if (!parse(argv[2], port) || !parse(argv[3], counter) || !parse(argv[4], delta) || port == 0)
        return 2;

    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 1;
    timeval timeout{1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    if (inet_pton(AF_INET, argv[1], &server.sin_addr) != 1) return 2;

    std::uint8_t bytes[32]{};
    write_u32(bytes, 0, kWireMagic);
    bytes[4] = kWireVersion;
    bytes[5] = kWireRequest;
    write_u64(bytes, 8, 1);
    write_u64(bytes, 16, delta);
    write_u64(bytes, 24, counter);
    if (sendto(fd, bytes, sizeof(bytes), 0, reinterpret_cast<const sockaddr*>(&server), sizeof(server)) !=
        static_cast<ssize_t>(sizeof(bytes)))
        return 1;
    const ssize_t received = recv(fd, bytes, sizeof(bytes), 0);
    close(fd);
    if (received != static_cast<ssize_t>(sizeof(bytes)) || read_u32(bytes, 0) != kWireMagic ||
        bytes[4] != kWireVersion || bytes[5] != kWireAck)
        return 1;
    const char* const outcome = bytes[6] == 1 ? "ACCEPT" : bytes[6] == 2 ? "MOVED" : "REJECT";
    std::cout << outcome
              << " request_id=" << read_u64(bytes, 8)
              << " observed=" << read_u64(bytes, 16) << '\n';
}
