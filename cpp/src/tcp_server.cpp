#include "counter_poc/tcp_server.hpp"

#include <arpa/inet.h>
#include <charconv>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace counter_poc {
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
    ConnectionSession(int fd, CounterEngine& engine, const std::string& strict_owner_endpoint) noexcept
        : fd_(fd), engine_(engine), strict_owner_endpoint_(strict_owner_endpoint),
          route_sequence_(static_cast<std::uint64_t>(fd) << 32U) {}

    void run() noexcept {
        char buffer[4096];
        std::size_t used = 0;
        while (true) {
            const ssize_t bytes = read(fd_, buffer + used, sizeof(buffer) - used);
            if (bytes < 0 && errno == EINTR) continue;
            if (bytes <= 0) break;

            used += static_cast<std::size_t>(bytes);
            std::size_t begin = 0;
            for (std::size_t index = 0; index < used; ++index) {
                if (buffer[index] != '\n') continue;
                const Result result = parse_and_apply(buffer + begin, buffer + index);
                bool sent = false;
                if (result.decision == Decision::Moved && !strict_owner_endpoint_.empty()) {
                    const std::string response = "M " + strict_owner_endpoint_ + "\n";
                    sent = write_all(fd_, response.data(), response.size());
                } else {
                    const char response[2] = {result.decision == Decision::Accepted
                                                  ? 'A'
                                                  : result.decision == Decision::Moved ? 'M' : 'R',
                                              '\n'};
                    sent = write_all(fd_, response, sizeof(response));
                }
                if (!sent) {
                    close(fd_);
                    return;
                }
                begin = index + 1;
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
    Result parse_and_apply(const char* begin, const char* end) noexcept {
        Amount delta = 0;
        const auto [parsed_end, error] = std::from_chars(begin, end, delta);
        if (error != std::errc{} || parsed_end != end)
            return {Decision::Rejected, 0, 0};
        return engine_.apply({0, delta, route_sequence_++});
    }

    int fd_;
    CounterEngine& engine_;
    const std::string& strict_owner_endpoint_;
    std::uint64_t route_sequence_;
};

}  // namespace

TcpCounterServer::TcpCounterServer(CounterRuntime& runtime, TcpServerConfig config)
    : runtime_(runtime), config_(std::move(config)) {}

void TcpCounterServer::run() {
    if (!runtime_.started()) throw std::logic_error("counter runtime must be started before TCP serving");
    const int listener = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(config_.port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (listener < 0 || setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0 ||
        bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
        listen(listener, 4096) != 0) {
        const int error = errno;
        if (listener >= 0) close(listener);
        throw std::system_error(error, std::generic_category(), "cannot start TCP listener");
    }
    for (;;) {
        const int client = accept(listener, nullptr, nullptr);
        if (client >= 0) {
            std::thread([client, &engine = runtime_.engine(), endpoint = config_.strict_owner_endpoint] {
                ConnectionSession(client, engine, endpoint).run();
            }).detach();
        } else if (errno != EINTR) {
            close(listener);
            throw std::system_error(errno, std::generic_category(), "TCP accept failed");
        }
    }
}

}  // namespace counter_poc
