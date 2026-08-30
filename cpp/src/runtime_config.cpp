#include "counter_poc/runtime_config.hpp"

#include <charconv>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace counter_poc {
namespace {

template <typename Number>
bool parse_number(std::string_view text, Number& destination) noexcept {
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), destination);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_replication_peer(std::string_view text, UdpPeer& peer) {
    const std::size_t separator = text.rfind(':');
    if (separator == std::string_view::npos || separator == 0 || separator + 1 == text.size())
        return false;
    std::uint16_t port = 0;
    if (!parse_number(text.substr(separator + 1), port) || port == 0) return false;
    peer = {std::string(text.substr(0, separator)), port};
    return true;
}

bool parse_reservation(std::string_view text, ComponentReservation& reservation) noexcept {
    const std::size_t separator = text.find(':');
    return separator != std::string_view::npos && separator != 0 && separator + 1 != text.size() &&
           parse_number(text.substr(0, separator), reservation.component_id) &&
           parse_number(text.substr(separator + 1), reservation.capacity);
}

bool parse_handoff_peer(std::string_view text, DistributedPeer& peer) {
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

std::vector<std::uint8_t> read_secret_file(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot open handoff authentication key file");
    const std::vector<char> file_bytes((std::istreambuf_iterator<char>(input)),
                                       std::istreambuf_iterator<char>());
    if (input.bad() || file_bytes.size() < 16)
        throw std::runtime_error("handoff authentication key must contain at least 16 bytes");
    std::vector<std::uint8_t> secret;
    secret.reserve(file_bytes.size());
    for (const char byte : file_bytes) secret.push_back(static_cast<std::uint8_t>(byte));
    return secret;
}

}  // namespace

bool parse_runtime_option(std::string_view argument, RuntimeLaunchOptions& options) {
    if (argument == "--start-peak") {
        options.start_peak = true;
        return true;
    }
    const auto value_after = [&argument](std::string_view name) -> std::string_view {
        return argument.starts_with(name) ? argument.substr(name.size()) : std::string_view{};
    };
    if (const std::string_view value = value_after("--limit="); !value.empty()) {
        return parse_number(value, options.limit) && options.limit != 0;
    }
    if (const std::string_view value = value_after("--shards="); !value.empty()) {
        return parse_number(value, options.shards) && options.shards != 0;
    }
    if (const std::string_view value = value_after("--danger-threshold="); !value.empty()) {
        return parse_number(value, options.danger_threshold);
    }
    if (const std::string_view value = value_after("--hot-tps="); !value.empty()) {
        return parse_number(value, options.hot_threshold_tps) && options.hot_threshold_tps != 0;
    }
    if (const std::string_view value = value_after("--component-id="); !value.empty()) {
        return parse_number(value, options.component_id);
    }
    if (const std::string_view value = value_after("--peak-reservation="); !value.empty()) {
        return parse_number(value, options.peak_reservation) && options.peak_reservation != 0;
    }
    if (const std::string_view value = value_after("--cluster-reservation="); !value.empty()) {
        ComponentReservation reservation{};
        if (!parse_reservation(value, reservation)) return false;
        options.cluster_reservations.push_back(reservation);
        return true;
    }
    if (const std::string_view value = value_after("--udp-bind-port="); !value.empty()) {
        return parse_number(value, options.replication_bind_port);
    }
    if (const std::string_view value = value_after("--udp-bind-host="); !value.empty()) {
        options.replication_bind_host = value;
        return true;
    }
    if (const std::string_view value = value_after("--udp-peer="); !value.empty()) {
        UdpPeer peer{};
        if (!parse_replication_peer(value, peer)) return false;
        options.replication_peers.push_back(std::move(peer));
        return true;
    }
    if (const std::string_view value = value_after("--handoff-bind-port="); !value.empty()) {
        options.handoff_option_seen = true;
        return parse_number(value, options.handoff_bind_port) && options.handoff_bind_port != 0;
    }
    if (const std::string_view value = value_after("--handoff-bind-host="); !value.empty()) {
        options.handoff_option_seen = true;
        options.handoff_bind_host = value;
        return true;
    }
    if (const std::string_view value = value_after("--handoff-leader="); !value.empty()) {
        options.handoff_option_seen = true;
        return parse_number(value, options.handoff_leader);
    }
    if (const std::string_view value = value_after("--strict-owner="); !value.empty()) {
        options.handoff_option_seen = true;
        return parse_number(value, options.strict_owner);
    }
    if (const std::string_view value = value_after("--handoff-peer="); !value.empty()) {
        options.handoff_option_seen = true;
        DistributedPeer peer{};
        if (!parse_handoff_peer(value, peer)) return false;
        options.handoff_peers.push_back(std::move(peer));
        return true;
    }
    if (const std::string_view value = value_after("--handoff-auth-key-file="); !value.empty()) {
        options.handoff_option_seen = true;
        options.handoff_auth_key_file = value;
        return true;
    }
    if (const std::string_view value = value_after("--handoff-journal="); !value.empty()) {
        options.handoff_option_seen = true;
        options.handoff_journal_path = value;
        return true;
    }
    return false;
}

CounterRuntimeConfig make_runtime_config(const RuntimeLaunchOptions& options) {
    if (options.shards == 0 || options.hot_threshold_tps == 0 ||
        options.component_id > std::numeric_limits<std::uint16_t>::max())
        throw std::invalid_argument("invalid counter runtime configuration");

    Amount local_reservation = options.peak_reservation;
    if (local_reservation > options.limit)
        throw std::invalid_argument("peak reservation exceeds the global limit");
    if (!options.cluster_reservations.empty()) {
        const ReservationPlan plan(options.limit, options.cluster_reservations);
        if (!plan.has_component(options.component_id))
            throw std::invalid_argument("cluster plan does not allocate this component id");
        const Amount allocated = plan.capacity_for(options.component_id);
        if (allocated == 0)
            throw std::invalid_argument("cluster component reservation must be non-zero");
        if (local_reservation != 0 && local_reservation != allocated)
            throw std::invalid_argument("local peak reservation disagrees with cluster plan");
        if (!options.start_peak)
            throw std::invalid_argument("a cluster escrow node must start in peak mode");
        local_reservation = allocated;
    }
    if (local_reservation != 0 && local_reservation != options.limit && !options.start_peak)
        throw std::invalid_argument("a component reservation requires initial peak mode");

    CounterRuntimeConfig config{{options.limit, options.shards, options.danger_threshold,
                                 options.hot_threshold_tps, options.component_id, local_reservation},
                                options.start_peak, std::nullopt, std::nullopt};
    if (options.replication_bind_port != 0 || !options.replication_peers.empty()) {
        config.replication = UdpReplicationConfig{options.component_id, options.replication_bind_port,
                                                  options.replication_peers, std::chrono::milliseconds(10),
                                                  8, 65536, options.replication_bind_host};
    }
    if (options.handoff_bind_port == 0) {
        if (options.handoff_option_seen)
            throw std::invalid_argument("handoff options require --handoff-bind-port");
        return config;
    }
    if (options.handoff_auth_key_file.empty() || options.handoff_journal_path.empty())
        throw std::invalid_argument(
            "distributed handoff requires --handoff-auth-key-file and --handoff-journal");
    if (options.cluster_reservations.empty())
        throw std::invalid_argument("distributed handoff requires a fixed cluster reservation plan");

    DistributedHandoffConfig handoff{};
    handoff.component_id = options.component_id;
    handoff.leader_component_id = options.handoff_leader;
    handoff.strict_owner_component_id = options.strict_owner;
    handoff.bind_port = options.handoff_bind_port;
    handoff.bind_host = options.handoff_bind_host;
    handoff.peers = options.handoff_peers;
    handoff.authentication_key = read_secret_file(options.handoff_auth_key_file);
    handoff.journal_path = options.handoff_journal_path;
    handoff.members.reserve(options.cluster_reservations.size());
    for (const ComponentReservation& reservation : options.cluster_reservations)
        handoff.members.push_back(reservation.component_id);
    config.handoff = std::move(handoff);
    return config;
}

}  // namespace counter_poc
