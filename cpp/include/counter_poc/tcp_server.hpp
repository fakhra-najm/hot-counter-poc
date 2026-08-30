#pragma once

#include "counter_poc/runtime.hpp"

#include <cstdint>
#include <string>

namespace counter_poc {

struct TcpServerConfig {
    std::uint16_t port{9090};
    // Sent only with a MOVED response. It identifies a TCP strict owner, for
    // example "10.0.0.11:9090".
    std::string strict_owner_endpoint;
};

class TcpCounterServer final {
public:
    TcpCounterServer(CounterRuntime& runtime, TcpServerConfig config);
    void run();  // Blocks until the process is stopped.

private:
    CounterRuntime& runtime_;
    TcpServerConfig config_;
};

}  // namespace counter_poc
