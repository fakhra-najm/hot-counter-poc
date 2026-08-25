#include "counter_poc/engine.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <system_error>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
namespace {
bool write_all(int fd, const char* data, std::size_t size) noexcept {
    std::size_t written = 0;
    while (written < size) {
        const ssize_t bytes = write(fd, data + written, size - written);
        if (bytes > 0) {
            written += static_cast<std::size_t>(bytes);
            continue;
        }
        if (bytes < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

class ConnectionSession final {
public:
    ConnectionSession(int fd, counter_poc::CounterEngine& engine) noexcept
        : fd_(fd), engine_(engine), route_sequence_(static_cast<std::uint64_t>(fd) << 32U) {}

    void run() noexcept {
        char buffer[4096];
        std::size_t used = 0;
        while (true) {
            const ssize_t bytes = read(fd_, buffer + used, sizeof(buffer) - used);
            if (bytes < 0 && errno == EINTR) continue;
            if (bytes <= 0) break;

            used += static_cast<std::size_t>(bytes);
            std::size_t begin = 0;
            for (std::size_t i = 0; i < used; ++i) {
                if (buffer[i] != '\n') continue;

                const counter_poc::Result result = parse_and_apply(buffer + begin, buffer + i);
                const char response[2] = {
                    result.decision == counter_poc::Decision::Accepted ? 'A' : 'R', '\n'};
                if (!write_all(fd_, response, sizeof(response))) {
                    close(fd_);
                    return;
                }
                begin = i + 1;
            }
            if (begin != 0) {
                used -= begin;
                std::memmove(buffer, buffer + begin, used);
            }
            if (used == sizeof(buffer)) break;
        }
        close(fd_);
    }

private:
    counter_poc::Result parse_and_apply(const char* begin, const char* end) noexcept {
        counter_poc::Amount delta = 0;
        const auto [parsed_end, error] = std::from_chars(begin, end, delta);
        if (error != std::errc{} || parsed_end != end)
            return {counter_poc::Decision::Rejected, 0, 0};
        return engine_.apply({0, delta, route_sequence_++});
    }

    int fd_;
    counter_poc::CounterEngine& engine_;
    std::uint64_t route_sequence_;
};

void serve_connection(int fd, counter_poc::CounterEngine& engine) {
    ConnectionSession(fd, engine).run();
}

}  // namespace

int main(int argc, char** argv) {
    const int port = argc > 1 ? std::atoi(argv[1]) : 9090;
    const auto limit = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1000000000000000ULL;
    const auto shards = argc > 3 ? static_cast<std::uint32_t>(std::atoi(argv[3])) : 3;
    const auto danger = argc > 4 ? std::strtoull(argv[4], nullptr, 10) : 100000ULL;
    const auto hot_tps = argc > 5 ? std::strtoull(argv[5], nullptr, 10) : 50000ULL;
    const bool start_peak = argc > 6 && std::strcmp(argv[6], "peak") == 0;

    if (port <= 0 || port > 65535 || shards == 0) {
        std::cerr << "invalid port or shard count\n";
        return 2;
    }

    std::signal(SIGPIPE, SIG_IGN);
    counter_poc::CounterEngine engine(limit, shards, danger, hot_tps);
    if (start_peak && !engine.enter_reserved_mode_after_quiescence()) {
        std::cerr << "cannot enter initial peak mode\n";
        return 2;
    }
    engine.start_control_plane();
    const int listener = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (listener < 0 || setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) ||
        bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
        listen(listener, 4096) < 0) {
        std::perror("listen");
        return 1;
    }

    std::cout << "counterd port=" << port << " limit=" << limit << " shards=" << shards
              << " danger_threshold=" << danger << '\n';
    for (;;) {
        const int client = accept(listener, nullptr, nullptr);
        if (client >= 0) std::thread(serve_connection, client, std::ref(engine)).detach();
    }
}
