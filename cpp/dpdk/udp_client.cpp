// Minimal kernel-UDP client for exercising the DPDK server's 40-byte request
// protocol. It is not a benchmark; use it first to verify ARP and replies.
#include "counter_poc/udp_counter_protocol.hpp"

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

    auto bytes = counter_poc::UdpCounterProtocol::encode_request({1, delta, counter});
    if (sendto(fd, bytes.data(), bytes.size(), 0, reinterpret_cast<const sockaddr*>(&server), sizeof(server)) !=
        static_cast<ssize_t>(bytes.size()))
        return 1;
    const ssize_t received = recv(fd, bytes.data(), bytes.size(), 0);
    close(fd);
    counter_poc::UdpCounterProtocol::Reply reply{};
    if (received != static_cast<ssize_t>(bytes.size()) ||
        !counter_poc::UdpCounterProtocol::decode_reply(bytes, reply))
        return 1;
    const char* const outcome = reply.result.decision == counter_poc::Decision::Accepted
                                    ? "ACCEPT"
                                    : reply.result.decision == counter_poc::Decision::Moved ? "MOVED" : "REJECT";
    std::cout << outcome
              << " request_id=" << reply.request_id
              << " observed=" << reply.result.observed;
    if (reply.result.decision == counter_poc::Decision::Moved && reply.strict_owner_ipv4 != 0) {
        in_addr owner{};
        owner.s_addr = htonl(reply.strict_owner_ipv4);
        char text[INET_ADDRSTRLEN]{};
        if (inet_ntop(AF_INET, &owner, text, sizeof(text)) != nullptr)
            std::cout << " redirect=" << text << ':'
                      << reply.strict_owner_port;
    }
    std::cout << '\n';
}
