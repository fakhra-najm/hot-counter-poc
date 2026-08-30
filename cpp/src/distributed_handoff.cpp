#include "counter_poc/distributed_handoff.hpp"
#include "counter_poc/auth.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
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
constexpr std::size_t kWirePayloadSize = 48;
constexpr std::size_t kFrameSize = kWirePayloadSize + ControlAuthenticator::kTagSize;
constexpr std::size_t kJournalPayloadSize = 48;
constexpr std::size_t kJournalRecordSize = kJournalPayloadSize + ControlAuthenticator::kTagSize;
constexpr std::uint32_t kJournalMagic = 0x48434a31U;  // HCJ1
constexpr std::uint8_t kJournalVersion = 1;

enum class WireType : std::uint8_t { Trigger = 1, Prepare = 2, Prepared = 3, Commit = 4,
                                     CommitAck = 5, Abort = 6 };

template <std::size_t Size>
void write_u16(std::array<std::uint8_t, Size>& bytes, std::size_t offset,
               std::uint16_t value) noexcept {
    bytes[offset] = static_cast<std::uint8_t>(value >> 8U);
    bytes[offset + 1] = static_cast<std::uint8_t>(value);
}

template <std::size_t Size>
void write_u32(std::array<std::uint8_t, Size>& bytes, std::size_t offset,
               std::uint32_t value) noexcept {
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        const std::size_t shift = (sizeof(value) - 1U - index) * 8U;
        bytes[offset + index] = static_cast<std::uint8_t>(value >> shift);
    }
}

template <std::size_t Size>
void write_u64(std::array<std::uint8_t, Size>& bytes, std::size_t offset,
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

enum class JournalPhase : std::uint8_t { Idle = 0, Prepared = 1, Committed = 2 };

struct JournalRecord {
    JournalPhase phase{JournalPhase::Idle};
    std::uint64_t term{};
    DrainedPeakState state{};
    Amount total{};
    std::uint16_t strict_owner{};
};

std::array<std::uint8_t, kFrameSize> encode_frame(WireType type, std::uint16_t sender,
                                                   std::uint64_t term, CounterId counter,
                                                   std::uint64_t epoch, Amount value,
                                                   std::uint16_t strict_owner,
                                                   std::span<const std::uint8_t> key) noexcept {
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
    const ControlAuthenticator::Tag tag =
        ControlAuthenticator::sign(key, std::span<const std::uint8_t>(bytes.data(), kWirePayloadSize));
    std::copy(tag.begin(), tag.end(), bytes.begin() + kWirePayloadSize);
    return bytes;
}

bool decode_frame(const std::uint8_t* bytes, std::size_t size, std::span<const std::uint8_t> key,
                  DecodedFrame& frame) noexcept {
    if (size != kFrameSize || read_u32(bytes, 0) != kMagic || bytes[4] != kVersion) return false;
    ControlAuthenticator::Tag tag{};
    std::copy_n(bytes + kWirePayloadSize, tag.size(), tag.begin());
    if (!ControlAuthenticator::verify(key,
                                      std::span<const std::uint8_t>(bytes, kWirePayloadSize), tag))
        return false;
    const std::uint8_t raw_type = bytes[5];
    if (raw_type < static_cast<std::uint8_t>(WireType::Trigger) ||
        raw_type > static_cast<std::uint8_t>(WireType::Abort))
        return false;
    frame = {static_cast<WireType>(raw_type), read_u16(bytes, 6), read_u64(bytes, 8),
             read_u64(bytes, 16), read_u64(bytes, 24), read_u64(bytes, 32),
             read_u16(bytes, 40)};
    return true;
}

class HandoffJournal final {
public:
    HandoffJournal(std::string path, std::span<const std::uint8_t> key)
        : path_(std::move(path)), key_(key.begin(), key.end()) {}

    enum class LoadResult : std::uint8_t { Missing, Loaded, Failed };

    LoadResult load(JournalRecord& record) const noexcept {
        const int fd = open(path_.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) return errno == ENOENT ? LoadResult::Missing : LoadResult::Failed;
        std::array<std::uint8_t, kJournalRecordSize> bytes{};
        const bool complete = read_exact(fd, bytes.data(), bytes.size());
        const bool closed = close(fd) == 0;
        const bool valid = complete && closed &&
                           read_u32(bytes.data(), 0) == kJournalMagic &&
                           bytes[4] == kJournalVersion &&
                           bytes[5] <= static_cast<std::uint8_t>(JournalPhase::Committed);
        if (!valid) return LoadResult::Failed;
        ControlAuthenticator::Tag tag{};
        std::copy_n(bytes.data() + kJournalPayloadSize, tag.size(), tag.begin());
        if (!ControlAuthenticator::verify(key_,
                                          std::span<const std::uint8_t>(bytes.data(),
                                                                        kJournalPayloadSize),
                                          tag))
            return LoadResult::Failed;
        record = {static_cast<JournalPhase>(bytes[5]), read_u64(bytes.data(), 8),
                  {read_u64(bytes.data(), 16), read_u64(bytes.data(), 24), 0},
                  read_u64(bytes.data(), 32), read_u16(bytes.data(), 40)};
        record.state.component_total = record.total;
        return LoadResult::Loaded;
    }

    bool store(const JournalRecord& record) const noexcept {
        std::array<std::uint8_t, kJournalRecordSize> bytes{};
        write_u32(bytes, 0, kJournalMagic);
        bytes[4] = kJournalVersion;
        bytes[5] = static_cast<std::uint8_t>(record.phase);
        write_u64(bytes, 8, record.term);
        write_u64(bytes, 16, record.state.counter);
        write_u64(bytes, 24, record.state.epoch);
        write_u64(bytes, 32, record.total);
        write_u16(bytes, 40, record.strict_owner);
        const ControlAuthenticator::Tag tag = ControlAuthenticator::sign(
            key_, std::span<const std::uint8_t>(bytes.data(), kJournalPayloadSize));
        std::copy(tag.begin(), tag.end(), bytes.begin() + kJournalPayloadSize);

        std::string temporary = path_ + ".tmp.XXXXXX";
        std::vector<char> mutable_name(temporary.begin(), temporary.end());
        mutable_name.push_back('\0');
        const int fd = mkstemp(mutable_name.data());
        if (fd < 0) return false;
        const bool durable = fchmod(fd, 0600) == 0 &&
                             write_all(fd, bytes.data(), bytes.size()) && fsync(fd) == 0;
        const bool closed = close(fd) == 0;
        const bool written = durable && closed;
        if (!written) {
            unlink(mutable_name.data());
            return false;
        }
        if (rename(mutable_name.data(), path_.c_str()) != 0) {
            unlink(mutable_name.data());
            return false;
        }
        const std::size_t separator = path_.find_last_of('/');
        const std::string parent = separator == std::string::npos ? "." :
                                   separator == 0 ? "/" : path_.substr(0, separator);
        const int parent_fd = open(parent.c_str(), O_RDONLY | O_CLOEXEC);
        if (parent_fd < 0) return false;
        const bool synced = fsync(parent_fd) == 0;
        close(parent_fd);
        return synced;
    }

private:
    static bool read_exact(int fd, std::uint8_t* bytes, std::size_t size) noexcept {
        std::size_t read_total = 0;
        while (read_total < size) {
            const ssize_t received = read(fd, bytes + read_total, size - read_total);
            if (received > 0) {
                read_total += static_cast<std::size_t>(received);
                continue;
            }
            if (received < 0 && errno == EINTR) continue;
            return false;
        }
        std::uint8_t extra{};
        for (;;) {
            const ssize_t received = read(fd, &extra, 1);
            if (received == 0) return true;
            if (received < 0 && errno == EINTR) continue;
            return false;
        }
    }

    static bool write_all(int fd, const std::uint8_t* bytes, std::size_t size) noexcept {
        std::size_t written = 0;
        while (written < size) {
            const ssize_t sent = write(fd, bytes + written, size - written);
            if (sent > 0) {
                written += static_cast<std::size_t>(sent);
                continue;
            }
            if (sent < 0 && errno == EINTR) continue;
            return false;
        }
        return true;
    }

    std::string path_;
    std::vector<std::uint8_t> key_;
};

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
    enum class State : std::uint8_t {
        Idle,
        LeaderPreparing,
        FollowerPrepared,
        // A commit was received, but it cannot be acknowledged until its
        // durable record and local strict mode both succeed. The gate stays
        // closed; timing out into peak mode here would be unsafe.
        FollowerCommitting,
        // The leader could not prove that its commit record reached stable
        // storage. It must not broadcast a decision or reopen admissions.
        LeaderCommitBlocked,
        LeaderCommitting
    };

    Impl(CounterEngine& input_engine, DistributedHandoffConfig input_config)
        : engine(input_engine), config(std::move(input_config)) {}

    bool is_member(std::uint16_t component) const noexcept {
        return members.contains(component);
    }

    bool is_leader() const noexcept { return config.component_id == config.leader_component_id; }

    bool persist(JournalPhase phase, std::uint64_t term, const DrainedPeakState& state,
                 Amount total) noexcept {
        return journal != nullptr && journal->store(
            {phase, term, state, total, static_cast<std::uint16_t>(config.strict_owner_component_id)});
    }

    void send(WireType type, const sockaddr_in& destination, std::uint64_t term, CounterId counter,
              std::uint64_t epoch, Amount value) noexcept {
        const auto bytes = encode_frame(type, static_cast<std::uint16_t>(config.component_id), term,
                                        counter, epoch, value,
                                        static_cast<std::uint16_t>(config.strict_owner_component_id),
                                        config.authentication_key);
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
        const std::uint64_t after_active = active_term == std::numeric_limits<std::uint64_t>::max()
                                               ? active_term
                                               : active_term + 1;
        const std::uint64_t after_commit =
            last_committed_term == std::numeric_limits<std::uint64_t>::max()
                ? last_committed_term
                : last_committed_term + 1;
        active_term = std::max({after_active, after_commit, monotonic_ns()});
        if (!persist(JournalPhase::Prepared, active_term, local, local.component_total)) {
            engine.abort_distributed_handoff();
            active_term = 0;
            return;
        }
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
            const DrainedPeakState empty{};
            (void)persist(JournalPhase::Idle, 0, empty, 0);
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
        if (total > engine.limit()) {
            abort_prepare(true);
            return;
        }
        if (!persist(JournalPhase::Committed, active_term, leader_state, total)) {
            state = State::LeaderCommitBlocked;
            return;
        }
        committed_total = total;
        committed_counter = leader_state.counter;
        committed_epoch = leader_state.epoch;
        commit_acks.clear();
        if (engine.commit_distributed_handoff(total,
                                              config.component_id == config.strict_owner_component_id))
            commit_acks.insert(static_cast<std::uint16_t>(config.component_id));
        state = State::LeaderCommitting;
        next_retry = std::chrono::steady_clock::time_point::min();
        commits.fetch_add(1, std::memory_order_relaxed);
    }

    void handle_frame(const DecodedFrame& frame, const sockaddr_in& source) noexcept {
        if (recovery_fenced) return;
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
            // Once a commit record is being installed, the admission gate is
            // deliberately frozen. A later prepare must not erase that state.
            if (state == State::FollowerCommitting || state == State::LeaderCommitBlocked ||
                state == State::LeaderCommitting)
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
            if (!persist(JournalPhase::Prepared, active_term, local, local.component_total)) {
                engine.abort_distributed_handoff();
                active_term = 0;
                return;
            }
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
            if ((state != State::FollowerPrepared && state != State::FollowerCommitting) ||
                frame.term != active_term)
                return;
            state = State::FollowerCommitting;
            const bool owner = config.component_id == config.strict_owner_component_id;
            const DrainedPeakState committed{frame.counter, frame.epoch, frame.value};
            if (!persist(JournalPhase::Committed, frame.term, committed, frame.value)) return;
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
            if (decode_frame(bytes.data(), static_cast<std::size_t>(received), config.authentication_key,
                             frame))
                handle_frame(frame, source);
        }
    }

    void tick() noexcept {
        const auto now = std::chrono::steady_clock::now();
        if (!recovery_fenced && state == State::Idle && engine.mode() == RoutingMode::ReservedPeak &&
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
    std::unique_ptr<HandoffJournal> journal;
    bool recovery_fenced{false};
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
    if (impl_->config.authentication_key.size() < 16)
        throw std::invalid_argument("handoff authentication key must contain at least 16 bytes");
    if (impl_->config.journal_path.empty())
        throw std::invalid_argument("handoff journal path is required");

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

    impl_->journal = std::make_unique<HandoffJournal>(impl_->config.journal_path,
                                                       impl_->config.authentication_key);
    JournalRecord recovered{};
    switch (impl_->journal->load(recovered)) {
        case HandoffJournal::LoadResult::Missing: {
            const JournalRecord idle{};
            if (!impl_->journal->store(idle))
                throw std::runtime_error("cannot initialize handoff journal");
            break;
        }
        case HandoffJournal::LoadResult::Loaded:
            if (recovered.strict_owner != impl_->config.strict_owner_component_id ||
                recovered.total > impl_->engine.limit())
                throw std::runtime_error("handoff journal conflicts with runtime configuration");
            if (recovered.phase == JournalPhase::Prepared) {
                // Do not guess whether the old leader committed. The process
                // must remain unavailable until the record is reconciled.
                impl_->engine.fence_after_interrupted_handoff();
                impl_->recovery_fenced = true;
            } else if (recovered.phase == JournalPhase::Committed) {
                if (!impl_->engine.recover_committed_handoff(
                        recovered.state, recovered.total,
                        impl_->config.component_id == impl_->config.strict_owner_component_id))
                    throw std::runtime_error("cannot restore committed handoff");
                impl_->last_committed_term = recovered.term;
            }
            break;
        case HandoffJournal::LoadResult::Failed:
            throw std::runtime_error("cannot authenticate or read handoff journal");
    }

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
