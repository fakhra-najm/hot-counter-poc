#include "counter_poc/runtime.hpp"
#include "counter_poc/runtime_config.hpp"
#include "counter_poc/tcp_server.hpp"

#include <charconv>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string_view>
#include <unistd.h>
#include <vector>

namespace {

struct Options {
    std::uint16_t port{9090};
    counter_poc::RuntimeLaunchOptions runtime;
    std::string strict_owner_endpoint;
};

template <typename Number>
bool parse_number(std::string_view text, Number& destination) noexcept {
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), destination);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_options(int argc, char** argv, Options& options) {
    std::uint32_t positional = 0;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "peak") {
            options.runtime.start_peak = true;
            continue;
        }
        if (argument.starts_with("--strict-owner-endpoint=")) {
            const std::string_view endpoint =
                argument.substr(std::string_view("--strict-owner-endpoint=").size());
            if (endpoint.empty()) return false;
            options.strict_owner_endpoint = endpoint;
            continue;
        }
        if (argument.starts_with("--")) {
            if (!counter_poc::parse_runtime_option(argument, options.runtime)) return false;
            continue;
        }
        bool valid = false;
        switch (positional++) {
            case 0: valid = parse_number(argument, options.port); break;
            case 1: valid = parse_number(argument, options.runtime.limit); break;
            case 2: valid = parse_number(argument, options.runtime.shards); break;
            case 3: valid = parse_number(argument, options.runtime.danger_threshold); break;
            case 4: valid = parse_number(argument, options.runtime.hot_threshold_tps); break;
            default: return false;
        }
        if (!valid) return false;
    }
    return options.port != 0;
}

void print_usage() {
    std::cerr << "usage: counterd [port limit shards danger-threshold hot-tps] [peak]"
                 " [--start-peak] [--limit=N] [--shards=N] [--danger-threshold=N] [--hot-tps=N]"
                 " [--component-id=N] [--peak-reservation=N]"
                 " [--cluster-reservation=component:capacity]..."
                 " [--udp-bind-port=N] [--udp-bind-host=IPv4] [--udp-peer=IPv4:port]..."
                 " [--handoff-bind-port=N] [--handoff-bind-host=IPv4]"
                 " [--handoff-leader=N] [--strict-owner=N]"
                 " [--handoff-peer=component:IPv4:port]..."
                 " [--handoff-auth-key-file=PATH] [--handoff-journal=PATH]"
                 " [--strict-owner-endpoint=IPv4:TCP_PORT]\n";
}

class LogLimitReachedPublisher final : public counter_poc::ILimitReachedPublisher {
public:
    void publish(const counter_poc::LimitReachedEvent& event) noexcept override {
        std::cerr << "LIMIT_REACHED counter=" << event.counter << " limit=" << event.limit
                  << " epoch=" << event.epoch << '\n';
    }
};

int launch_dpdk_adapter(int argc, char** argv) {
    const std::string_view invoked_as(argv[0]);
    const std::size_t separator = invoked_as.find_last_of('/');
    std::string executable = separator == std::string_view::npos
                                 ? "./counterd_dpdk"
                                 : std::string(invoked_as.substr(0, separator + 1)) + "counterd_dpdk";
    std::vector<char*> child_arguments(argv, argv + argc);
    child_arguments[0] = executable.data();
    child_arguments.push_back(nullptr);
    execv(executable.c_str(), child_arguments.data());
    std::cerr << "cannot launch DPDK adapter '" << executable << "': " << std::strerror(errno) << '\n';
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    const char* const transport = std::getenv("COUNTER_TRANSPORT");
    if (transport != nullptr && std::string_view(transport) == "dpdk")
        return launch_dpdk_adapter(argc, argv);
    if (transport != nullptr && std::string_view(transport) != "tcp") {
        std::cerr << "COUNTER_TRANSPORT must be tcp or dpdk\n";
        return 2;
    }
    Options options{};
    if (!parse_options(argc, argv, options)) {
        print_usage();
        return 2;
    }

    std::signal(SIGPIPE, SIG_IGN);
    try {
        LogLimitReachedPublisher limit_events;
        counter_poc::CounterRuntime runtime(counter_poc::make_runtime_config(options.runtime));
        runtime.engine().set_limit_reached_publisher(&limit_events);
        runtime.start();
        std::cout << "counterd transport=tcp port=" << options.port
                  << " limit=" << runtime.engine().limit()
                  << " shards=" << options.runtime.shards
                  << " danger_threshold=" << options.runtime.danger_threshold
                  << " component_id=" << options.runtime.component_id
                  << " peak_reservation=" << runtime.engine().local_peak_reservation();
        if (runtime.replication_port() != 0) std::cout << " replication_port=" << runtime.replication_port();
        if (runtime.handoff_port() != 0) std::cout << " handoff_port=" << runtime.handoff_port();
        std::cout << '\n';
        counter_poc::TcpCounterServer server(runtime, {options.port, options.strict_owner_endpoint});
        server.run();
    } catch (const std::exception& error) {
        std::cerr << "cannot start TCP counter runtime: " << error.what() << '\n';
        return 2;
    }
}
