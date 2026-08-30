#include "counter_poc/distributed_handoff.hpp"
#include "counter_poc/engine.hpp"
#include "counter_poc/replication.hpp"
#include "counter_poc/reservation_plan.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <system_error>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
namespace {
using counter_poc::Amount;

struct ServerOptions {
    std::uint16_t port{9090};
    Amount limit{1000000000000000ULL};
    std::uint32_t shards{3};
    Amount danger{100000ULL};
    std::uint64_t hot_tps{50000ULL};
    bool start_peak{false};
    std::uint32_t component_id{0};
    Amount peak_reservation{0};
    std::vector<counter_poc::ComponentReservation> cluster_reservations;
    std::uint16_t udp_bind_port{0};
    std::string udp_bind_host{"0.0.0.0"};
    std::vector<counter_poc::UdpPeer> udp_peers;
    std::uint16_t handoff_bind_port{0};
    std::string handoff_bind_host{"0.0.0.0"};
    std::uint32_t handoff_leader{0};
    std::uint32_t strict_owner{0};
    std::vector<counter_poc::DistributedPeer> handoff_peers;
};

template <typename Number>
bool parse_number(std::string_view text, Number& destination) noexcept {
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), destination);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_peer(std::string_view text, counter_poc::UdpPeer& peer) {
    const std::size_t separator = text.rfind(':');
    if (separator == std::string_view::npos || separator == 0 || separator + 1 == text.size())
        return false;
    std::uint16_t port = 0;
    if (!parse_number(text.substr(separator + 1), port) || port == 0) return false;
    peer = {std::string(text.substr(0, separator)), port};
    return true;
}

bool parse_reservation(std::string_view text, counter_poc::ComponentReservation& reservation) noexcept {
    const std::size_t separator = text.find(':');
    return separator != std::string_view::npos && separator != 0 && separator + 1 != text.size() &&
           parse_number(text.substr(0, separator), reservation.component_id) &&
           parse_number(text.substr(separator + 1), reservation.capacity);
}

bool parse_handoff_peer(std::string_view text, counter_poc::DistributedPeer& peer) {
    const std::size_t first_separator = text.find(':');
    const std::size_t last_separator = text.rfind(':');
    if (first_separator == std::string_view::npos || first_separator == 0 ||
        first_separator == last_separator || last_separator + 1 == text.size())
        return false;
    std::uint16_t port = 0;
    if (!parse_number(text.substr(0, first_separator), peer.component_id) ||
        !parse_number(text.substr(last_separator + 1), port) || port == 0)
        return false;
    peer.host = std::string(text.substr(first_separator + 1, last_separator - first_separator - 1));
    peer.port = port;
    return !peer.host.empty();
}

bool parse_options(int argc, char** argv, ServerOptions& options) {
    std::uint32_t positional = 0;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "peak" || argument == "--start-peak") {
            options.start_peak = true;
            continue;
        }
        const auto value_after = [&argument](std::string_view name) -> std::string_view {
            return argument.starts_with(name) ? argument.substr(name.size()) : std::string_view{};
        };
        if (const std::string_view value = value_after("--component-id="); !value.empty()) {
            if (!parse_number(value, options.component_id)) return false;
        } else if (const std::string_view value = value_after("--peak-reservation="); !value.empty()) {
            if (!parse_number(value, options.peak_reservation) || value == "0") return false;
        } else if (const std::string_view value = value_after("--cluster-reservation="); !value.empty()) {
            counter_poc::ComponentReservation reservation{};
            if (!parse_reservation(value, reservation)) return false;
            options.cluster_reservations.push_back(reservation);
        } else if (const std::string_view value = value_after("--udp-bind-port="); !value.empty()) {
            if (!parse_number(value, options.udp_bind_port)) return false;
        } else if (const std::string_view value = value_after("--udp-bind-host="); !value.empty()) {
            options.udp_bind_host = value;
        } else if (const std::string_view value = value_after("--udp-peer="); !value.empty()) {
            counter_poc::UdpPeer peer{};
            if (!parse_peer(value, peer)) return false;
            options.udp_peers.push_back(std::move(peer));
        } else if (const std::string_view value = value_after("--handoff-bind-port="); !value.empty()) {
            if (!parse_number(value, options.handoff_bind_port) || options.handoff_bind_port == 0)
                return false;
        } else if (const std::string_view value = value_after("--handoff-bind-host="); !value.empty()) {
            options.handoff_bind_host = value;
        } else if (const std::string_view value = value_after("--handoff-leader="); !value.empty()) {
            if (!parse_number(value, options.handoff_leader)) return false;
        } else if (const std::string_view value = value_after("--strict-owner="); !value.empty()) {
            if (!parse_number(value, options.strict_owner)) return false;
        } else if (const std::string_view value = value_after("--handoff-peer="); !value.empty()) {
            counter_poc::DistributedPeer peer{};
            if (!parse_handoff_peer(value, peer)) return false;
            options.handoff_peers.push_back(std::move(peer));
        } else if (argument.starts_with("--")) {
            return false;
        } else {
            bool valid = false;
            switch (positional++) {
                case 0: valid = parse_number(argument, options.port); break;
                case 1: valid = parse_number(argument, options.limit); break;
                case 2: valid = parse_number(argument, options.shards); break;
                case 3: valid = parse_number(argument, options.danger); break;
                case 4: valid = parse_number(argument, options.hot_tps); break;
                default: return false;
            }
            if (!valid) return false;
        }
    }
    return options.port != 0 && options.shards != 0 && options.hot_tps != 0 &&
           options.component_id <= std::numeric_limits<std::uint16_t>::max() &&
           !(options.peak_reservation != 0 && options.peak_reservation > options.limit) &&
           !(options.peak_reservation != 0 && options.peak_reservation != options.limit &&
             !options.start_peak);
}

void print_usage() {
    std::cerr << "usage: counterd [port limit shards danger-threshold hot-tps] [peak]"
                 " [--component-id=N] [--peak-reservation=N] [--udp-bind-port=N]"
                 " [--cluster-reservation=component:capacity]..."
                 " [--udp-bind-host=IPv4] [--udp-peer=IPv4:port]"
                 " [--handoff-bind-port=N] [--handoff-bind-host=IPv4]"
                 " [--handoff-leader=N] [--strict-owner=N]"
                 " [--handoff-peer=component:IPv4:port]...\n";
}

class LogLimitReachedPublisher final : public counter_poc::ILimitReachedPublisher {
public:
    void publish(const counter_poc::LimitReachedEvent& event) noexcept override {
        std::cerr << "LIMIT_REACHED counter=" << event.counter << " limit=" << event.limit
                  << " epoch=" << event.epoch << '\n';
    }
};

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
                const char response[2] = {result.decision == counter_poc::Decision::Accepted
                                               ? 'A'
                                               : result.decision == counter_poc::Decision::Moved ? 'M' : 'R',
                                          '\n'};
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
    ServerOptions options{};
    if (!parse_options(argc, argv, options)) {
        print_usage();
        return 2;
    }

    try {
        if (!options.cluster_reservations.empty()) {
            const counter_poc::ReservationPlan plan(options.limit, options.cluster_reservations);
            if (!plan.has_component(options.component_id)) {
                std::cerr << "cluster plan does not allocate this component id\n";
                return 2;
            }
            const Amount allocated = plan.capacity_for(options.component_id);
            if (allocated == 0) {
                std::cerr << "cluster component reservation must be non-zero\n";
                return 2;
            }
            if (options.peak_reservation != 0 && options.peak_reservation != allocated) {
                std::cerr << "local peak reservation disagrees with cluster plan\n";
                return 2;
            }
            options.peak_reservation = allocated;
            if (!options.start_peak) {
                std::cerr << "a cluster escrow node must start in peak mode\n";
                return 2;
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "invalid cluster reservation plan: " << error.what() << '\n';
        return 2;
    }

    std::signal(SIGPIPE, SIG_IGN);
    counter_poc::CounterEngine engine(
        {options.limit, options.shards, options.danger, options.hot_tps, options.component_id,
         options.peak_reservation});
    LogLimitReachedPublisher limit_events;
    engine.set_limit_reached_publisher(&limit_events);

    std::unique_ptr<counter_poc::ReliableUdpReplicator> replicator;
    if (options.udp_bind_port != 0 || !options.udp_peers.empty()) {
        counter_poc::UdpReplicationConfig config{};
        config.component_id = options.component_id;
        config.bind_port = options.udp_bind_port;
        config.bind_host = options.udp_bind_host;
        config.peers = options.udp_peers;
        try {
            replicator = std::make_unique<counter_poc::ReliableUdpReplicator>(std::move(config));
            replicator->start();
            engine.set_replication_publisher(replicator.get());
        } catch (const std::exception& error) {
            std::cerr << "cannot start UDP replication: " << error.what() << '\n';
            return 2;
        }
    }

    if (options.start_peak && !engine.enter_reserved_mode_after_quiescence()) {
        std::cerr << "cannot enter initial peak mode\n";
        return 2;
    }
    engine.start_control_plane();

    std::unique_ptr<counter_poc::DistributedHandoffController> handoff;
    if (options.handoff_bind_port != 0) {
        counter_poc::DistributedHandoffConfig config{};
        config.component_id = options.component_id;
        config.leader_component_id = options.handoff_leader;
        config.strict_owner_component_id = options.strict_owner;
        config.bind_port = options.handoff_bind_port;
        config.bind_host = options.handoff_bind_host;
        config.peers = options.handoff_peers;
        for (const auto& reservation : options.cluster_reservations)
            config.members.push_back(reservation.component_id);
        try {
            handoff = std::make_unique<counter_poc::DistributedHandoffController>(engine,
                                                                                   std::move(config));
            handoff->start();
        } catch (const std::exception& error) {
            std::cerr << "cannot start distributed handoff: " << error.what() << '\n';
            return 2;
        }
    } else if (!options.handoff_peers.empty()) {
        std::cerr << "handoff peers require --handoff-bind-port\n";
        return 2;
    }
    const int listener = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(options.port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (listener < 0 || setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) ||
        bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
        listen(listener, 4096) < 0) {
        std::perror("listen");
        return 1;
    }

    std::cout << "counterd port=" << options.port << " limit=" << options.limit
              << " shards=" << options.shards << " danger_threshold=" << options.danger
              << " component_id=" << options.component_id
              << " peak_reservation=" << engine.local_peak_reservation();
    if (replicator != nullptr) std::cout << " udp_port=" << replicator->bound_port();
    if (handoff != nullptr) std::cout << " handoff_port=" << handoff->bound_port();
    std::cout << '\n';
    for (;;) {
        const int client = accept(listener, nullptr, nullptr);
        if (client >= 0) std::thread(serve_connection, client, std::ref(engine)).detach();
    }
}
