// Optional Linux/DPDK data plane. It is intentionally a separate executable:
// the normal daemon remains a transparent kernel-TCP baseline, while this
// executable owns a dedicated NIC through a DPDK poll-mode driver.
#include "counter_poc/engine.hpp"

extern "C" {
#include <rte_arp.h>
#include <rte_byteorder.h>
#include <rte_eal.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_udp.h>
}

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <string_view>
#include <vector>

namespace {

constexpr std::uint32_t kWireMagic = 0x48435031U;  // HCP1
constexpr std::uint8_t kWireVersion = 1;
constexpr std::uint8_t kWireRequest = 1;
constexpr std::uint8_t kWireAck = 2;
constexpr std::size_t kWireSize = 32;
constexpr std::uint16_t kRxDescriptors = 1024;
constexpr std::uint16_t kTxDescriptors = 1024;
constexpr std::uint16_t kBurstSize = 32;

volatile std::sig_atomic_t g_stop = 0;

struct Options {
    std::uint16_t dpdk_port{0};
    std::uint16_t udp_port{9091};
    std::uint16_t queues{1};
    counter_poc::Amount limit{1000000000000000ULL};
    std::uint32_t shards{3};
    counter_poc::Amount danger{100000ULL};
    std::uint64_t hot_tps{50000ULL};
    bool start_peak{false};
};

struct WorkerContext {
    std::uint16_t port;
    std::uint16_t queue;
    std::uint16_t udp_port;
    counter_poc::CounterEngine* engine;
};

template <typename Number>
bool parse_number(std::string_view text, Number& destination) noexcept {
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), destination);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_options(int argc, char** argv, Options& options) {
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--start-peak") {
            options.start_peak = true;
            continue;
        }
        const auto value_after = [&argument](std::string_view name) -> std::string_view {
            return argument.starts_with(name) ? argument.substr(name.size()) : std::string_view{};
        };
        if (const auto value = value_after("--dpdk-port="); !value.empty()) {
            if (!parse_number(value, options.dpdk_port)) return false;
        } else if (const auto value = value_after("--udp-port="); !value.empty()) {
            if (!parse_number(value, options.udp_port) || options.udp_port == 0) return false;
        } else if (const auto value = value_after("--queues="); !value.empty()) {
            if (!parse_number(value, options.queues) || options.queues == 0) return false;
        } else if (const auto value = value_after("--limit="); !value.empty()) {
            if (!parse_number(value, options.limit)) return false;
        } else if (const auto value = value_after("--shards="); !value.empty()) {
            if (!parse_number(value, options.shards) || options.shards == 0) return false;
        } else if (const auto value = value_after("--danger-threshold="); !value.empty()) {
            if (!parse_number(value, options.danger)) return false;
        } else if (const auto value = value_after("--hot-tps="); !value.empty()) {
            if (!parse_number(value, options.hot_tps) || options.hot_tps == 0) return false;
        } else {
            return false;
        }
    }
    return true;
}

void usage() {
    std::cerr << "usage: counterd_dpdk EAL_OPTIONS -- [--dpdk-port=N] [--udp-port=N]"
                 " [--queues=N] [--limit=N] [--shards=N] [--danger-threshold=N]"
                 " [--hot-tps=N] [--start-peak]\n";
}

std::uint32_t read_u32(const std::uint8_t* source, std::size_t offset) noexcept {
    return (static_cast<std::uint32_t>(source[offset]) << 24U) |
           (static_cast<std::uint32_t>(source[offset + 1]) << 16U) |
           (static_cast<std::uint32_t>(source[offset + 2]) << 8U) |
           static_cast<std::uint32_t>(source[offset + 3]);
}

std::uint64_t read_u64(const std::uint8_t* source, std::size_t offset) noexcept {
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < sizeof(value); ++index)
        value = (value << 8U) | source[offset + index];
    return value;
}

void write_u64(std::uint8_t* destination, std::size_t offset, std::uint64_t value) noexcept {
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        const std::size_t shift = (sizeof(value) - 1U - index) * 8U;
        destination[offset + index] = static_cast<std::uint8_t>(value >> shift);
    }
}

void reverse_ethernet(rte_ether_hdr* ethernet) noexcept {
    const rte_ether_addr source = ethernet->src_addr;
    ethernet->src_addr = ethernet->dst_addr;
    ethernet->dst_addr = source;
}

bool reply_arp(rte_mbuf* packet) noexcept {
    if (rte_pktmbuf_pkt_len(packet) < sizeof(rte_ether_hdr) + sizeof(rte_arp_hdr)) return false;
    auto* ethernet = rte_pktmbuf_mtod(packet, rte_ether_hdr*);
    if (ethernet->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_ARP)) return false;
    auto* arp = rte_pktmbuf_mtod_offset(packet, rte_arp_hdr*, sizeof(rte_ether_hdr));
    if (arp->arp_opcode != rte_cpu_to_be_16(RTE_ARP_OP_REQUEST)) return false;

    const rte_ether_addr requester_mac = arp->arp_data.arp_sha;
    const rte_be32_t requester_ip = arp->arp_data.arp_sip;
    const rte_ether_addr responder_mac = arp->arp_data.arp_tha;
    const rte_be32_t responder_ip = arp->arp_data.arp_tip;
    reverse_ethernet(ethernet);
    arp->arp_opcode = rte_cpu_to_be_16(RTE_ARP_OP_REPLY);
    arp->arp_data.arp_sha = responder_mac;
    arp->arp_data.arp_sip = responder_ip;
    arp->arp_data.arp_tha = requester_mac;
    arp->arp_data.arp_tip = requester_ip;
    return true;
}

bool process_udp_request(rte_mbuf* packet, const WorkerContext& context) noexcept {
    if (packet->nb_segs != 1 ||
        rte_pktmbuf_pkt_len(packet) < sizeof(rte_ether_hdr) + sizeof(rte_ipv4_hdr) +
                                     sizeof(rte_udp_hdr) + kWireSize)
        return false;

    auto* ethernet = rte_pktmbuf_mtod(packet, rte_ether_hdr*);
    if (ethernet->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4)) return false;
    auto* ip = rte_pktmbuf_mtod_offset(packet, rte_ipv4_hdr*, sizeof(rte_ether_hdr));
    const std::size_t ip_length = static_cast<std::size_t>(ip->version_ihl & 0x0FU) * 4U;
    if ((ip->version_ihl >> 4U) != 4U || ip_length < sizeof(rte_ipv4_hdr) ||
        ip->next_proto_id != IPPROTO_UDP ||
        rte_pktmbuf_pkt_len(packet) < sizeof(rte_ether_hdr) + ip_length + sizeof(rte_udp_hdr) +
                                     kWireSize)
        return false;

    auto* udp = rte_pktmbuf_mtod_offset(packet, rte_udp_hdr*, sizeof(rte_ether_hdr) + ip_length);
    if (rte_be_to_cpu_16(udp->dst_port) != context.udp_port) return false;
    auto* payload = rte_pktmbuf_mtod_offset(packet, std::uint8_t*,
                                            sizeof(rte_ether_hdr) + ip_length + sizeof(rte_udp_hdr));
    if (read_u32(payload, 0) != kWireMagic || payload[4] != kWireVersion ||
        payload[5] != kWireRequest)
        return false;

    const std::uint64_t request_id = read_u64(payload, 8);
    const counter_poc::Amount delta = read_u64(payload, 16);
    const counter_poc::CounterId counter = read_u64(payload, 24);
    const counter_poc::Result result = context.engine->apply({counter, delta, request_id});

    reverse_ethernet(ethernet);
    const rte_be32_t source_ip = ip->src_addr;
    ip->src_addr = ip->dst_addr;
    ip->dst_addr = source_ip;
    const rte_be16_t source_port = udp->src_port;
    udp->src_port = udp->dst_port;
    udp->dst_port = source_port;
    payload[5] = kWireAck;
    payload[6] = result.decision == counter_poc::Decision::Accepted
                     ? 1U
                     : result.decision == counter_poc::Decision::Moved ? 2U : 0U;
    payload[7] = 0;
    write_u64(payload, 8, request_id);
    write_u64(payload, 16, result.observed);
    write_u64(payload, 24, counter);
    ip->hdr_checksum = 0;
    ip->hdr_checksum = rte_ipv4_cksum(ip);
    // A zero UDP checksum is valid for IPv4. It avoids a software checksum in
    // this reference fast path; production must enable and verify NIC TX
    // checksum offload before relying on it.
    udp->dgram_cksum = 0;
    return true;
}

int worker_main(void* argument) {
    const auto& context = *static_cast<WorkerContext*>(argument);
    std::array<rte_mbuf*, kBurstSize> received{};
    std::array<rte_mbuf*, kBurstSize> replies{};
    while (g_stop == 0) {
        const std::uint16_t count =
            rte_eth_rx_burst(context.port, context.queue, received.data(), received.size());
        if (count == 0) {
            rte_pause();
            continue;
        }
        std::uint16_t reply_count = 0;
        for (std::uint16_t index = 0; index < count; ++index) {
            rte_mbuf* packet = received[index];
            if (reply_arp(packet) || process_udp_request(packet, context)) {
                replies[reply_count++] = packet;
            } else {
                rte_pktmbuf_free(packet);
            }
        }
        const std::uint16_t transmitted =
            rte_eth_tx_burst(context.port, context.queue, replies.data(), reply_count);
        for (std::uint16_t index = transmitted; index < reply_count; ++index)
            rte_pktmbuf_free(replies[index]);
    }
    return 0;
}

void stop_handler(int) { g_stop = 1; }

int setup_port(std::uint16_t port, std::uint16_t queues, rte_mempool* pool) {
    rte_eth_dev_info info{};
    if (rte_eth_dev_info_get(port, &info) != 0) return -1;
    if (queues > info.max_rx_queues || queues > info.max_tx_queues) return -1;

    rte_eth_conf configuration{};
    if (queues > 1) {
        const std::uint64_t wanted_rss = RTE_ETH_RSS_IP | RTE_ETH_RSS_UDP;
        configuration.rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
        configuration.rx_adv_conf.rss_conf.rss_hf = wanted_rss & info.flow_type_rss_offloads;
        if (configuration.rx_adv_conf.rss_conf.rss_hf == 0) return -1;
    }
    int status = rte_eth_dev_configure(port, queues, queues, &configuration);
    if (status != 0) return status;

    std::uint16_t rx_descriptors = kRxDescriptors;
    std::uint16_t tx_descriptors = kTxDescriptors;
    status = rte_eth_dev_adjust_nb_rx_tx_desc(port, &rx_descriptors, &tx_descriptors);
    if (status != 0) return status;
    for (std::uint16_t queue = 0; queue < queues; ++queue) {
        rte_eth_rxconf rx_config = info.default_rxconf;
        status = rte_eth_rx_queue_setup(port, queue, rx_descriptors, rte_eth_dev_socket_id(port),
                                        &rx_config, pool);
        if (status != 0) return status;
        rte_eth_txconf tx_config = info.default_txconf;
        status = rte_eth_tx_queue_setup(port, queue, tx_descriptors, rte_eth_dev_socket_id(port),
                                        &tx_config);
        if (status != 0) return status;
    }
    status = rte_eth_dev_start(port);
    if (status != 0) return status;
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const int consumed = rte_eal_init(argc, argv);
    if (consumed < 0) return 2;
    argc -= consumed;
    argv += consumed;

    Options options{};
    if (!parse_options(argc, argv, options) ||
        options.dpdk_port >= rte_eth_dev_count_avail() || options.queues > rte_lcore_count()) {
        usage();
        return 2;
    }

    const int socket = rte_eth_dev_socket_id(options.dpdk_port);
    const unsigned pool_size = std::max<unsigned>(8192, options.queues * (kRxDescriptors + kTxDescriptors));
    rte_mempool* pool = rte_pktmbuf_pool_create("counterd_dpdk_pool", pool_size, 256, 0,
                                                RTE_MBUF_DEFAULT_BUF_SIZE, socket);
    if (pool == nullptr || setup_port(options.dpdk_port, options.queues, pool) != 0) {
        std::cerr << "cannot configure DPDK port/queues\n";
        return 1;
    }

    counter_poc::CounterEngine engine(options.limit, options.shards, options.danger, options.hot_tps);
    if (options.start_peak && !engine.enter_reserved_mode_after_quiescence()) {
        std::cerr << "cannot enter initial peak mode\n";
        return 2;
    }
    engine.start_control_plane();
    std::signal(SIGINT, stop_handler);
    std::signal(SIGTERM, stop_handler);

    std::vector<WorkerContext> contexts;
    contexts.reserve(options.queues);
    for (std::uint16_t queue = 0; queue < options.queues; ++queue)
        contexts.push_back({options.dpdk_port, queue, options.udp_port, &engine});

    std::uint16_t queue = 1;
    unsigned lcore_id = 0;
    RTE_LCORE_FOREACH_WORKER(lcore_id) {
        if (queue >= options.queues) break;
        if (rte_eal_remote_launch(worker_main, &contexts[queue], lcore_id) != 0) {
            std::cerr << "cannot launch DPDK worker\n";
            g_stop = 1;
            break;
        }
        ++queue;
    }
    if (queue != options.queues) {
        std::cerr << "not enough enabled DPDK lcores for requested queues\n";
        g_stop = 1;
    }
    if (g_stop == 0) worker_main(&contexts[0]);
    g_stop = 1;
    RTE_LCORE_FOREACH_WORKER(lcore_id) {
        rte_eal_wait_lcore(lcore_id);
    }
    engine.stop_control_plane();
    rte_eth_dev_stop(options.dpdk_port);
    rte_eth_dev_close(options.dpdk_port);
    rte_eal_cleanup();
    return 0;
}
