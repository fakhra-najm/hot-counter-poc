#include "counter_poc/replication.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

namespace counter_poc {
namespace {

constexpr std::uint32_t kMagic = 0x43504331U;  // "CPC1"
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kFrameSize = 40;

enum class WireType : std::uint8_t { Snapshot = 1, Ack = 2 };

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
    return static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(bytes[offset]) << 8U) | bytes[offset + 1]);
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
    std::uint16_t component;
    std::uint64_t sequence;
    CounterId counter;
    std::uint64_t epoch;
    Amount value;
};

std::array<std::uint8_t, kFrameSize> encode_frame(
    WireType type, std::uint16_t component, std::uint64_t sequence, CounterId counter,
    std::uint64_t epoch, Amount value) noexcept {
    std::array<std::uint8_t, kFrameSize> bytes{};
    write_u32(bytes, 0, kMagic);
    bytes[4] = kVersion;
    bytes[5] = static_cast<std::uint8_t>(type);
    write_u16(bytes, 6, component);
    write_u64(bytes, 8, sequence);
    write_u64(bytes, 16, counter);
    write_u64(bytes, 24, epoch);
    write_u64(bytes, 32, value);
    return bytes;
}

bool decode_frame(const std::uint8_t* bytes, std::size_t size, DecodedFrame& frame) noexcept {
    if (size != kFrameSize || read_u32(bytes, 0) != kMagic || bytes[4] != kVersion)
        return false;
    if (bytes[5] != static_cast<std::uint8_t>(WireType::Snapshot) &&
        bytes[5] != static_cast<std::uint8_t>(WireType::Ack))
        return false;
    frame = {static_cast<WireType>(bytes[5]), read_u16(bytes, 6), read_u64(bytes, 8),
             read_u64(bytes, 16), read_u64(bytes, 24), read_u64(bytes, 32)};
    return true;
}

sockaddr_in resolve_ipv4(const UdpPeer& peer) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* result = nullptr;
    const std::string port = std::to_string(peer.port);
    const int status = getaddrinfo(peer.host.c_str(), port.c_str(), &hints, &result);
    if (status != 0 || result == nullptr) throw std::invalid_argument("cannot resolve UDP peer");
    sockaddr_in endpoint = *reinterpret_cast<sockaddr_in*>(result->ai_addr);
    freeaddrinfo(result);
    return endpoint;
}

sockaddr_in resolve_bind_ipv4(const std::string& host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_NUMERICHOST;
    addrinfo* result = nullptr;
    const std::string service = std::to_string(port);
    const int status = getaddrinfo(host.c_str(), service.c_str(), &hints, &result);
    if (status != 0 || result == nullptr)
        throw std::invalid_argument("cannot resolve UDP bind address");
    sockaddr_in endpoint = *reinterpret_cast<sockaddr_in*>(result->ai_addr);
    freeaddrinfo(result);
    return endpoint;
}

}  // namespace

struct ReliableUdpReplicator::Impl {
    struct ComponentKey {
        CounterId counter;
        std::uint64_t epoch;
        std::uint32_t component;

        bool operator==(const ComponentKey& other) const noexcept {
            return counter == other.counter && epoch == other.epoch && component == other.component;
        }
    };

    struct ComponentKeyHash {
        std::size_t operator()(const ComponentKey& key) const noexcept {
            std::size_t value = static_cast<std::size_t>(key.counter);
            value ^= static_cast<std::size_t>(key.epoch + 0x9e3779b97f4a7c15ULL + (value << 6U) +
                                              (value >> 2U));
            value ^= static_cast<std::size_t>(key.component + 0x9e3779b9U + (value << 6U) +
                                              (value >> 2U));
            return value;
        }
    };

    struct Pending {
        std::array<std::uint8_t, kFrameSize> bytes;
        sockaddr_in peer;
        std::chrono::steady_clock::time_point retry_at;
        std::uint32_t attempts;
    };

    explicit Impl(UdpReplicationConfig input) : config(std::move(input)) {}

    void send_bytes(const std::array<std::uint8_t, kFrameSize>& bytes,
                    const sockaddr_in& peer) noexcept {
        const ssize_t sent = sendto(fd, bytes.data(), bytes.size(), 0,
                                    reinterpret_cast<const sockaddr*>(&peer), sizeof(peer));
        if (sent == static_cast<ssize_t>(bytes.size())) sent_count.fetch_add(1, std::memory_order_relaxed);
    }

    void drain_outbound() noexcept {
        ReplicationUpdate update{};
        while (outbound.try_pop(update)) {
            const ComponentKey key{update.counter, update.epoch, update.component};
            const auto found = coalesced.find(key);
            if (found == coalesced.end()) {
                coalesced.emplace(key, update);
            } else if (update.value > found->second.value) {
                found->second = update;
            }
        }
        for (const auto& [key, newest] : coalesced) {
            (void)key;
            for (const sockaddr_in& peer : peers) {
                if (pending.size() >= config.max_pending) {
                    dropped_count.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                const std::uint64_t sequence = next_sequence++;
                const auto bytes = encode_frame(
                    WireType::Snapshot, static_cast<std::uint16_t>(newest.component), sequence,
                    newest.counter, newest.epoch, newest.value);
                send_bytes(bytes, peer);
                pending.emplace(sequence, Pending{bytes, peer,
                                                  std::chrono::steady_clock::now() +
                                                      config.retry_after,
                                                  0});
            }
        }
        coalesced.clear();
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
            if (!decode_frame(bytes.data(), static_cast<std::size_t>(received), frame)) continue;
            if (frame.type == WireType::Ack) {
                const auto found = pending.find(frame.sequence);
                if (found != pending.end() && found->second.peer.sin_addr.s_addr == source.sin_addr.s_addr &&
                    found->second.peer.sin_port == source.sin_port) {
                    pending.erase(found);
                    acknowledged_count.fetch_add(1, std::memory_order_relaxed);
                }
                continue;
            }

            {
                std::lock_guard<std::mutex> lock(component_mutex);
                const ComponentKey key{frame.counter, frame.epoch, frame.component};
                Amount& observed = components[key];
                observed = std::max(observed, frame.value);
            }
            received_count.fetch_add(1, std::memory_order_relaxed);
            const auto ack = encode_frame(WireType::Ack, static_cast<std::uint16_t>(config.component_id),
                                          frame.sequence, 0, 0, 0);
            send_bytes(ack, source);
        }
    }

    void retry_expired() noexcept {
        const auto now = std::chrono::steady_clock::now();
        for (auto it = pending.begin(); it != pending.end();) {
            Pending& pending_message = it->second;
            if (pending_message.retry_at > now) {
                ++it;
                continue;
            }
            if (pending_message.attempts >= config.max_retries) {
                dropped_count.fetch_add(1, std::memory_order_relaxed);
                it = pending.erase(it);
                continue;
            }
            send_bytes(pending_message.bytes, pending_message.peer);
            ++pending_message.attempts;
            pending_message.retry_at = now + config.retry_after;
            retried_count.fetch_add(1, std::memory_order_relaxed);
            ++it;
        }
    }

    void run() noexcept {
        while (!stop_requested.load(std::memory_order_acquire)) {
            drain_outbound();
            receive_all();
            retry_expired();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    UdpReplicationConfig config;
    MpmcQueue<ReplicationUpdate, 65536> outbound;
    std::vector<sockaddr_in> peers;
    std::unordered_map<std::uint64_t, Pending> pending;
    std::unordered_map<ComponentKey, ReplicationUpdate, ComponentKeyHash> coalesced;
    std::unordered_map<ComponentKey, Amount, ComponentKeyHash> components;
    mutable std::mutex component_mutex;
    std::atomic<bool> stop_requested{false};
    std::atomic<std::uint16_t> actual_port{0};
    std::atomic<std::uint64_t> queued_count{0};
    std::atomic<std::uint64_t> dropped_count{0};
    std::atomic<std::uint64_t> sent_count{0};
    std::atomic<std::uint64_t> acknowledged_count{0};
    std::atomic<std::uint64_t> retried_count{0};
    std::atomic<std::uint64_t> received_count{0};
    std::uint64_t next_sequence{1};
    int fd{-1};
    std::thread worker;
};

ReliableUdpReplicator::ReliableUdpReplicator(UdpReplicationConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

ReliableUdpReplicator::~ReliableUdpReplicator() { stop(); }

void ReliableUdpReplicator::start() {
    if (impl_->worker.joinable()) return;
    if (impl_->config.component_id > std::numeric_limits<std::uint16_t>::max())
        throw std::invalid_argument("UDP component id exceeds protocol range");
    if (impl_->config.max_pending == 0) throw std::invalid_argument("UDP max pending must be non-zero");
    impl_->peers.clear();
    impl_->pending.clear();
    impl_->coalesced.clear();
    impl_->next_sequence = 1;

    impl_->fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (impl_->fd < 0) throw std::runtime_error("cannot create UDP socket");

    const int flags = fcntl(impl_->fd, F_GETFL, 0);
    if (flags < 0 || fcntl(impl_->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(impl_->fd);
        impl_->fd = -1;
        throw std::runtime_error("cannot make UDP socket non-blocking");
    }

    const sockaddr_in bind_address =
        resolve_bind_ipv4(impl_->config.bind_host, impl_->config.bind_port);
    if (bind(impl_->fd, reinterpret_cast<const sockaddr*>(&bind_address), sizeof(bind_address)) < 0) {
        const int error = errno;
        close(impl_->fd);
        impl_->fd = -1;
        throw std::system_error(error, std::generic_category(), "cannot bind UDP socket");
    }

    sockaddr_in actual{};
    socklen_t actual_size = sizeof(actual);
    if (getsockname(impl_->fd, reinterpret_cast<sockaddr*>(&actual), &actual_size) < 0) {
        close(impl_->fd);
        impl_->fd = -1;
        throw std::runtime_error("cannot inspect UDP socket");
    }
    impl_->actual_port.store(ntohs(actual.sin_port), std::memory_order_release);

    try {
        impl_->peers.reserve(impl_->config.peers.size());
        for (const UdpPeer& peer : impl_->config.peers) impl_->peers.push_back(resolve_ipv4(peer));
    } catch (...) {
        close(impl_->fd);
        impl_->fd = -1;
        throw;
    }

    impl_->stop_requested.store(false, std::memory_order_release);
    impl_->worker = std::thread(&Impl::run, impl_.get());
}

void ReliableUdpReplicator::stop() noexcept {
    impl_->stop_requested.store(true, std::memory_order_release);
    if (impl_->worker.joinable()) impl_->worker.join();
    if (impl_->fd >= 0) {
        close(impl_->fd);
        impl_->fd = -1;
    }
}

void ReliableUdpReplicator::publish(const ReplicationUpdate& update) noexcept {
    if (impl_->outbound.try_push(update)) {
        impl_->queued_count.fetch_add(1, std::memory_order_relaxed);
    } else {
        impl_->dropped_count.fetch_add(1, std::memory_order_relaxed);
    }
}

Amount ReliableUdpReplicator::component_value(CounterId counter, std::uint64_t epoch,
                                              std::uint32_t component) const noexcept {
    std::lock_guard<std::mutex> lock(impl_->component_mutex);
    const auto found = impl_->components.find({counter, epoch, component});
    return found == impl_->components.end() ? 0 : found->second;
}

Amount ReliableUdpReplicator::merged_total(CounterId counter, std::uint64_t epoch) const noexcept {
    std::lock_guard<std::mutex> lock(impl_->component_mutex);
    Amount total = 0;
    for (const auto& [key, value] : impl_->components) {
        if (key.counter != counter || key.epoch != epoch) continue;
        if (value > std::numeric_limits<Amount>::max() - total)
            return std::numeric_limits<Amount>::max();
        total += value;
    }
    return total;
}

ReplicationStats ReliableUdpReplicator::stats() const noexcept {
    return {impl_->queued_count.load(std::memory_order_relaxed),
            impl_->dropped_count.load(std::memory_order_relaxed),
            impl_->sent_count.load(std::memory_order_relaxed),
            impl_->acknowledged_count.load(std::memory_order_relaxed),
            impl_->retried_count.load(std::memory_order_relaxed),
            impl_->received_count.load(std::memory_order_relaxed)};
}

std::uint16_t ReliableUdpReplicator::bound_port() const noexcept {
    return impl_->actual_port.load(std::memory_order_acquire);
}

}  // namespace counter_poc
