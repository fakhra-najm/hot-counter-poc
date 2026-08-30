#include "counter_poc/distributed_handoff.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace counter_poc {
namespace {

constexpr std::uint32_t kMagic = 0x48434e31U;  // HCN1
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kFrameSize = 48;

enum class WireType : std::uint8_t { Trigger = 1, Prepare = 2, Prepared = 3, Commit = 4,
                                     CommitAck = 5, Abort = 6 };

void write_u16(std::array<std::uint8_t, kFrameSize>& bytes, std::size_t offset,
               std::uint16_t value) noexcept {
    bytes[offset] = static_cast<std::uint8_t>(value >> 8U);
    bytes[offset + 1] = static_cast<std::uint8_t>(value);
}

void write_u32(std::array<std::uint8_t, kFrameSize>& bytes, std::size_t offset,
               std::uint32_t value) noexcept {
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        const std::size_t shift = (sizeof(value) - 1U - index) * 8U;
        bytes[offset + index] = static_cast<std::uint8_t>(value >> shift);
    }
}

void write_u64(std::array<std::uint8_t, kFrameSize>& bytes, std::size_t offset,
               std::uint64_t value) noexcept {
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

struct DecodedFrame {
    WireType type;
    std::uint16_t sender;
    std::uint64_t term;
    CounterId counter;
    std::uint64_t epoch;
    Amount value;
    std::uint16_t strict_owner;
};

std::array<std::uint8_t, kFrameSize> encode_frame(WireType type, std::uint16_t sender,
                                                   std::uint64_t term, CounterId counter,
                                                   std::uint64_t epoch, Amount value,
                                                   std::uint16_t strict_owner) noexcept {
    std::array<std::uint8_t, kFrameSize> bytes{};
    write_u32(bytes, 0, kMagic);
    bytes[4] = kVersion;
    bytes[5] = static_cast<std::uint8_t>(type);
    write_u16(bytes, 6, sender);
    write_u64(bytes, 8, term);
    write_u64(bytes, 16, counter);
    write_u64(bytes, 24, epoch);
    write_u64(bytes, 32, value);
    write_u16(bytes, 40, strict_owner);
    return bytes;
}

bool decode_frame(const std::uint8_t* bytes, std::size_t size, DecodedFrame& frame) noexcept {
    if (size != kFrameSize || read_u32(bytes, 0) != kMagic || bytes[4] != kVersion) return false;
    const std::uint8_t raw_type = bytes[5];
    if (raw_type < static_cast<std::uint8_t>(WireType::Trigger) ||
        raw_type > static_cast<std::uint8_t>(WireType::Abort))
        return false;
    frame = {static_cast<WireType>(raw_type), read_u16(bytes, 6), read_u64(bytes, 8),
             read_u64(bytes, 16), read_u64(bytes, 24), read_u64(bytes, 32),
             read_u16(bytes, 40)};
    return true;
}

sockaddr_in resolve_ipv4(const std::string& host, std::uint16_t port, bool bind_address) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    if (bind_address) hints.ai_flags = AI_NUMERICHOST;
    addrinfo* result = nullptr;
    const std::string service = std::to_string(port);
    const int status = getaddrinfo(host.c_str(), service.c_str(), &hints, &result);
    if (status != 0 || result == nullptr) throw std::invalid_argument("cannot resolve handoff endpoint");
    const sockaddr_in endpoint = *reinterpret_cast<sockaddr_in*>(result->ai_addr);
    freeaddrinfo(result);
    return endpoint;
}

bool same_endpoint(const sockaddr_in& left, const sockaddr_in& right) noexcept {
    return left.sin_addr.s_addr == right.sin_addr.s_addr && left.sin_port == right.sin_port;
}

std::uint64_t monotonic_ns() noexcept {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

}  // namespace

struct DistributedHandoffController::Impl {
    enum class State : std::uint8_t { Idle, LeaderPreparing, FollowerPrepared, LeaderCommitting };

    Impl(CounterEngine& input_engine, DistributedHandoffConfig input_config)
        : engine(input_engine), config(std::move(input_config)) {}

    bool is_member(std::uint16_t component) const noexcept {
        return members.contains(component);
    }

    bool is_leader() const noexcept { return config.component_id == config.leader_component_id; }

    void send(WireType type, const sockaddr_in& destination, std::uint64_t term, CounterId counter,
              std::uint64_t epoch, Amount value) noexcept {
        const auto bytes = encode_frame(type, static_cast<std::uint16_t>(config.component_id), term,
                                        counter, epoch, value,
                                        static_cast<std::uint16_t>(config.strict_owner_component_id));
        const ssize_t sent = sendto(fd, bytes.data(), bytes.size(), 0,
                                    reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
        (void)sent;
    }

    void broadcast(WireType type, std::uint64_t term, CounterId counter, std::uint64_t epoch,
                   Amount value) noexcept {
        for (const auto& [component, endpoint] : peers) {
            (void)component;
            send(type, endpoint, term, counter, epoch, value);
        }
    }

    void send_to_component(WireType type, std::uint16_t component, std::uint64_t term,
                           CounterId counter, std::uint64_t epoch, Amount value) noexcept {
        const auto found = peers.find(component);
        if (found != peers.end()) send(type, found->second, term, counter, epoch, value);
    }

    void begin_leader_prepare() noexcept {
        if (!is_leader() || state != State::Idle) return;
        DrainedPeakState local{};
        if (!engine.begin_distributed_handoff(local)) return;
        active_term = std::max(active_term + 1, monotonic_ns());
        prepared.clear();
        prepared.emplace(static_cast<std::uint16_t>(config.component_id), local);
        state = State::LeaderPreparing;
        prepare_deadline = std::chrono::steady_clock::now() + config.prepare_timeout;
        next_retry = std::chrono::steady_clock::time_point::min();
        prepares.fetch_add(1, std::memory_order_relaxed);
    }

    void abort_prepare(bool broadcast_abort) noexcept {
        if (state == State::LeaderPreparing || state == State::FollowerPrepared) {
            engine.abort_distributed_handoff();
            if (broadcast_abort && is_leader())
                broadcast(WireType::Abort, active_term, 0, 0, 0);
            aborts.fetch_add(1, std::memory_order_relaxed);
        }
        prepared.clear();
        state = State::Idle;
        active_term = 0;
    }

    bool all_prepared() const noexcept { return prepared.size() == members.size(); }
    bool all_commit_acked() const noexcept { return commit_acks.size() == members.size(); }

    void try_begin_commit() noexcept {
        if (state != State::LeaderPreparing || !all_prepared()) return;
        const DrainedPeakState& leader_state = prepared.at(static_cast<std::uint16_t>(config.component_id));
        Amount total = 0;
        for (const auto& [component, component_state] : prepared) {
            (void)component;
            if (component_state.epoch != leader_state.epoch ||
                component_state.counter != leader_state.counter ||
                component_state.component_total > std::numeric_limits<Amount>::max() - total) {
                abort_prepare(true);
                return;
            }
            total += component_state.component_total;
        }
        if (total > engine.limit() ||
            !engine.commit_distributed_handoff(
                total, config.component_id == config.strict_owner_component_id)) {
            abort_prepare(true);
            return;
        }
        committed_total = total;
        committed_counter = leader_state.counter;
        committed_epoch = leader_state.epoch;
        commit_acks.clear();
        commit_acks.insert(static_cast<std::uint16_t>(config.component_id));
        state = State::LeaderCommitting;
        next_retry = std::chrono::steady_clock::time_point::min();
        commits.fetch_add(1, std::memory_order_relaxed);
    }

    void handle_frame(const DecodedFrame& frame, const sockaddr_in& source) noexcept {
        if (frame.sender == config.component_id || !is_member(frame.sender)) return;
        const auto peer = peers.find(frame.sender);
        if (peer == peers.end() || !same_endpoint(peer->second, source)) return;

        if (frame.type == WireType::Trigger) {
            if (is_leader()) begin_leader_prepare();
            return;
        }
        if (frame.type == WireType::Prepare) {
            if (frame.sender != config.leader_component_id || frame.strict_owner != config.strict_owner_component_id)
                return;
            // Never let a delayed prepare roll a participant back from a
            // newer prepared/committed term.
            if ((state == State::FollowerPrepared && frame.term < active_term) ||
                frame.term < last_committed_term)
                return;
            if (state == State::FollowerPrepared && frame.term == active_term) {
                const DrainedPeakState& local = prepared.at(static_cast<std::uint16_t>(config.component_id));
                send_to_component(WireType::Prepared, static_cast<std::uint16_t>(config.leader_component_id),
                                  active_term, local.counter, local.epoch, local.component_total);
                return;
            }
            if (state != State::Idle) abort_prepare(false);
            DrainedPeakState local{};
            if (!engine.begin_distributed_handoff(local)) return;
            active_term = frame.term;
            prepared.clear();
            prepared.emplace(static_cast<std::uint16_t>(config.component_id), local);
            state = State::FollowerPrepared;
            prepare_deadline = std::chrono::steady_clock::now() + config.prepare_timeout;
            prepares.fetch_add(1, std::memory_order_relaxed);
            send_to_component(WireType::Prepared, static_cast<std::uint16_t>(config.leader_component_id),
                              active_term, local.counter, local.epoch, local.component_total);
            return;
        }
        if (frame.type == WireType::Prepared) {
            if (!is_leader() || state != State::LeaderPreparing || frame.term != active_term) return;
            prepared[frame.sender] = {frame.counter, frame.epoch, frame.value};
            try_begin_commit();
            return;
        }
        if (frame.type == WireType::Commit) {
            if (frame.sender != config.leader_component_id || frame.strict_owner != config.strict_owner_component_id)
                return;
            if (frame.term == last_committed_term) {
                send_to_component(WireType::CommitAck,
                                  static_cast<std::uint16_t>(config.leader_component_id), frame.term,
                                  frame.counter, frame.epoch, frame.value);
                return;
            }
            if (state != State::FollowerPrepared || frame.term != active_term) return;
            const bool owner = config.component_id == config.strict_owner_component_id;
            if (!engine.commit_distributed_handoff(frame.value, owner)) return;
            last_committed_term = frame.term;
            state = State::Idle;
            active_term = 0;
            commits.fetch_add(1, std::memory_order_relaxed);
            send_to_component(WireType::CommitAck,
                              static_cast<std::uint16_t>(config.leader_component_id), frame.term,
                              frame.counter, frame.epoch, frame.value);
            return;
        }
        if (frame.type == WireType::CommitAck) {
            if (!is_leader() || state != State::LeaderCommitting || frame.term != active_term) return;
            commit_acks.insert(frame.sender);
            if (all_commit_acked()) {
                last_committed_term = active_term;
                state = State::Idle;
                active_term = 0;
            }
            return;
        }
        if (frame.type == WireType::Abort && frame.sender == config.leader_component_id &&
            frame.term == active_term && state == State::FollowerPrepared) {
            abort_prepare(false);
        }
    }

    void receive_all() noexcept {
        for (;;) {
            std::array<std::uint8_t, kFrameSize> bytes{};
            sockaddr_in source{};
            socklen_t source_size = sizeof(source);
            const ssize_t received = recvfrom(fd, bytes.data(), bytes.size(), 0,
                                              reinterpret_cast<sockaddr*>(&source), &source_size);
            if (received < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR) continue;
                return;
            }
            DecodedFrame frame{};
            if (decode_frame(bytes.data(), static_cast<std::size_t>(received), frame))
                handle_frame(frame, source);
        }
    }

    void tick() noexcept {
        const auto now = std::chrono::steady_clock::now();
        if (state == State::Idle && engine.mode() == RoutingMode::ReservedPeak &&
            engine.local_peak_reservation() < engine.limit()) {
            const Amount local_total = engine.current_peak_total();
            const Amount local_capacity = engine.local_peak_reservation();
            if (local_capacity - local_total <= engine.danger_threshold()) {
                triggers.fetch_add(1, std::memory_order_relaxed);
                if (is_leader()) {
                    begin_leader_prepare();
                } else if (now >= next_trigger) {
                    send_to_component(WireType::Trigger,
                                      static_cast<std::uint16_t>(config.leader_component_id), 0, 0, 0, 0);
                    next_trigger = now + config.retry_after;
                }
            }
        }

        if (state == State::LeaderPreparing) {
            if (now >= prepare_deadline) {
                abort_prepare(true);
                return;
            }
            if (now >= next_retry) {
                const DrainedPeakState& local = prepared.at(static_cast<std::uint16_t>(config.component_id));
                broadcast(WireType::Prepare, active_term, local.counter, local.epoch, local.component_total);
                next_retry = now + config.retry_after;
            }
        } else if (state == State::FollowerPrepared) {
            if (now >= prepare_deadline) {
                abort_prepare(false);
                return;
            }
            if (now >= next_retry) {
                const DrainedPeakState& local = prepared.at(static_cast<std::uint16_t>(config.component_id));
                send_to_component(WireType::Prepared,
                                  static_cast<std::uint16_t>(config.leader_component_id), active_term,
                                  local.counter, local.epoch, local.component_total);
                next_retry = now + config.retry_after;
            }
        } else if (state == State::LeaderCommitting && now >= next_retry) {
            for (const auto& [component, endpoint] : peers) {
                if (!commit_acks.contains(component))
                    send(WireType::Commit, endpoint, active_term, committed_counter, committed_epoch,
                         committed_total);
            }
            next_retry = now + config.retry_after;
        }
    }

    void run() noexcept {
        while (!stop_requested.load(std::memory_order_acquire)) {
            receive_all();
            tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    CounterEngine& engine;
    DistributedHandoffConfig config;
    std::unordered_set<std::uint16_t> members;
    std::unordered_map<std::uint16_t, sockaddr_in> peers;
    std::unordered_map<std::uint16_t, DrainedPeakState> prepared;
    std::unordered_set<std::uint16_t> commit_acks;
    State state{State::Idle};
    std::uint64_t active_term{0};
    std::uint64_t last_committed_term{0};
    CounterId committed_counter{0};
    std::uint64_t committed_epoch{0};
    Amount committed_total{0};
    std::chrono::steady_clock::time_point prepare_deadline{};
    std::chrono::steady_clock::time_point next_retry{};
    std::chrono::steady_clock::time_point next_trigger{};
    std::atomic<bool> stop_requested{false};
    std::atomic<std::uint16_t> actual_port{0};
    std::atomic<std::uint64_t> triggers{0};
    std::atomic<std::uint64_t> prepares{0};
    std::atomic<std::uint64_t> commits{0};
    std::atomic<std::uint64_t> aborts{0};
    int fd{-1};
    std::thread worker;
};

DistributedHandoffController::DistributedHandoffController(CounterEngine& engine,
                                                           DistributedHandoffConfig config)
    : impl_(std::make_unique<Impl>(engine, std::move(config))) {}

DistributedHandoffController::~DistributedHandoffController() { stop(); }

void DistributedHandoffController::start() {
    if (impl_->worker.joinable()) return;
    if (impl_->config.bind_port == 0 || impl_->config.retry_after.count() <= 0 ||
        impl_->config.prepare_timeout <= impl_->config.retry_after)
        throw std::invalid_argument("invalid distributed handoff timing or bind port");
    if (impl_->config.component_id > std::numeric_limits<std::uint16_t>::max() ||
        impl_->config.leader_component_id > std::numeric_limits<std::uint16_t>::max() ||
        impl_->config.strict_owner_component_id > std::numeric_limits<std::uint16_t>::max())
        throw std::invalid_argument("handoff component id exceeds protocol range");

    impl_->members.clear();
    for (const std::uint32_t member : impl_->config.members) {
        if (member > std::numeric_limits<std::uint16_t>::max() ||
            !impl_->members.insert(static_cast<std::uint16_t>(member)).second)
            throw std::invalid_argument("invalid or duplicate handoff member");
    }
    if (!impl_->members.contains(static_cast<std::uint16_t>(impl_->config.component_id)) ||
        !impl_->members.contains(static_cast<std::uint16_t>(impl_->config.leader_component_id)) ||
        !impl_->members.contains(static_cast<std::uint16_t>(impl_->config.strict_owner_component_id)))
        throw std::invalid_argument("handoff membership omits a required component");

    impl_->peers.clear();
    for (const DistributedPeer& peer : impl_->config.peers) {
        if (peer.component_id > std::numeric_limits<std::uint16_t>::max() || peer.port == 0 ||
            peer.component_id == impl_->config.component_id ||
            !impl_->members.contains(static_cast<std::uint16_t>(peer.component_id)))
            throw std::invalid_argument("invalid handoff peer");
        const auto [it, inserted] = impl_->peers.emplace(
            static_cast<std::uint16_t>(peer.component_id), resolve_ipv4(peer.host, peer.port, false));
        (void)it;
        if (!inserted) throw std::invalid_argument("duplicate handoff peer");
    }
    if (impl_->peers.size() + 1 != impl_->members.size())
        throw std::invalid_argument("handoff peers do not cover fixed membership");

    impl_->fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (impl_->fd < 0) throw std::runtime_error("cannot create handoff UDP socket");
    const int flags = fcntl(impl_->fd, F_GETFL, 0);
    if (flags < 0 || fcntl(impl_->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(impl_->fd);
        impl_->fd = -1;
        throw std::runtime_error("cannot make handoff UDP socket non-blocking");
    }
    const sockaddr_in address = resolve_ipv4(impl_->config.bind_host, impl_->config.bind_port, true);
    if (bind(impl_->fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
        const int error = errno;
        close(impl_->fd);
        impl_->fd = -1;
        throw std::system_error(error, std::generic_category(), "cannot bind handoff UDP socket");
    }
    sockaddr_in actual{};
    socklen_t actual_size = sizeof(actual);
    if (getsockname(impl_->fd, reinterpret_cast<sockaddr*>(&actual), &actual_size) < 0) {
        close(impl_->fd);
        impl_->fd = -1;
        throw std::runtime_error("cannot inspect handoff UDP socket");
    }
    impl_->actual_port.store(ntohs(actual.sin_port), std::memory_order_release);
    impl_->stop_requested.store(false, std::memory_order_release);
    impl_->worker = std::thread(&Impl::run, impl_.get());
}

void DistributedHandoffController::stop() noexcept {
    impl_->stop_requested.store(true, std::memory_order_release);
    if (impl_->worker.joinable()) impl_->worker.join();
    // Stopping before commit is equivalent to a local abort: leave no
    // admission gate closed merely because the controller is being torn down.
    // A commit is intentionally never rolled back here.
    if (impl_->state == Impl::State::LeaderPreparing ||
        impl_->state == Impl::State::FollowerPrepared)
        impl_->abort_prepare(false);
    if (impl_->fd >= 0) {
        close(impl_->fd);
        impl_->fd = -1;
    }
}

DistributedHandoffStats DistributedHandoffController::stats() const noexcept {
    return {impl_->triggers.load(std::memory_order_relaxed),
            impl_->prepares.load(std::memory_order_relaxed),
            impl_->commits.load(std::memory_order_relaxed),
            impl_->aborts.load(std::memory_order_relaxed)};
}

std::uint16_t DistributedHandoffController::bound_port() const noexcept {
    return impl_->actual_port.load(std::memory_order_acquire);
}

}  // namespace counter_poc
