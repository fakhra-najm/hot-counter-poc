#pragma once

#include "counter_poc/distributed_handoff.hpp"
#include "counter_poc/engine.hpp"
#include "counter_poc/replication.hpp"
#include "counter_poc/reservation_plan.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace counter_poc {

// Transport-neutral command-line input. TCP and DPDK adapters parse their
// own listener options, then use this exact structure for the counter engine,
// replication, and distributed handoff setup.
struct RuntimeLaunchOptions {
    Amount limit{1000000000000000ULL};
    std::uint32_t shards{3};
    Amount danger_threshold{100000ULL};
    std::uint64_t hot_threshold_tps{50000ULL};
    bool start_peak{false};
    std::uint32_t component_id{0};
    Amount peak_reservation{0};
    std::vector<ComponentReservation> cluster_reservations;

    std::uint16_t replication_bind_port{0};
    std::string replication_bind_host{"0.0.0.0"};
    std::vector<UdpPeer> replication_peers;

    std::uint16_t handoff_bind_port{0};
    std::string handoff_bind_host{"0.0.0.0"};
    std::uint32_t handoff_leader{0};
    std::uint32_t strict_owner{0};
    std::vector<DistributedPeer> handoff_peers;
    std::string handoff_auth_key_file;
    std::string handoff_journal_path;
    bool handoff_option_seen{false};
};

struct CounterRuntimeConfig {
    CounterEngineConfig engine;
    bool start_peak{false};
    std::optional<UdpReplicationConfig> replication;
    std::optional<DistributedHandoffConfig> handoff;
};

// Parses one named, transport-neutral option such as --component-id=N or
// --handoff-peer=N:IPv4:port. Returns false for an unknown or malformed
// option; callers retain ownership of their transport-specific options.
bool parse_runtime_option(std::string_view argument, RuntimeLaunchOptions& options);

// Validates the static escrow plan, reads the handoff secret from its file,
// and produces a transport-independent runtime configuration. It throws
// std::invalid_argument or std::runtime_error on invalid configuration.
CounterRuntimeConfig make_runtime_config(const RuntimeLaunchOptions& options);

}  // namespace counter_poc
