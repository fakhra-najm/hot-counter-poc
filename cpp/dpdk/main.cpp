// Optional Linux/DPDK data plane. It is intentionally a separate executable:
// the normal daemon remains a transparent kernel-TCP baseline, while this
// executable owns a dedicated NIC through a DPDK poll-mode driver.
#include "counter_poc/runtime.hpp"
#include "counter_poc/runtime_config.hpp"
#include "counter_poc/udp_counter_protocol.hpp"

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

#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr std::size_t kWireSize = counter_poc::UdpCounterProtocol::kFrameSize;
constexpr std::uint16_t kRxDescriptors = 1024;
constexpr std::uint16_t kTxDescriptors = 1024;
constexpr std::uint16_t kBurstSize = 32;

volatile std::sig_atomic_t g_stop = 0;

struct Options {
    std::uint16_t dpdk_port{0};
    std::uint16_t udp_port{9091};
    std::uint16_t queues{1};
    counter_poc::RuntimeLaunchOptions runtime;
    std::string strict_owner_endpoint;
};

struct RedirectEndpoint {
    std::uint32_t ipv4_host_order{};
    std::uint16_t port{};
};

struct WorkerContext {
    std::uint16_t port;
    std::uint16_t queue;
    std::uint16_t udp_port;
    counter_poc::CounterEngine* engine;
    RedirectEndpoint redirect;
};

template <typename Number>
bool parse_number(std::string_view text, Number& destination) noexcept {
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), destination);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_options(int argc, char** argv, Options& options) {
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        const auto value_after = [&argument](std::string_view name) -> std::string_view {
            return argument.starts_with(name) ? argument.substr(name.size()) : std::string_view{};
        };
        if (const auto value = value_after("--dpdk-port="); !value.empty()) {
            if (!parse_number(value, options.dpdk_port)) return false;
        } else if (const auto value = value_after("--udp-port="); !value.empty()) {
            if (!parse_number(value, options.udp_port) || options.udp_port == 0) return false;
        } else if (const auto value = value_after("--queues="); !value.empty()) {
            if (!parse_number(value, options.queues) || options.queues == 0) return false;
        } else if (const auto value = value_after("--strict-owner-endpoint="); !value.empty()) {
            options.strict_owner_endpoint = value;
        } else if (!counter_poc::parse_runtime_option(argument, options.runtime)) {
            return false;
        } else {
            continue;
        }
    }
    return true;
}

void usage() {
    std::cerr << "usage: counterd_dpdk EAL_OPTIONS -- [--dpdk-port=N] [--udp-port=N]"
                 " [--queues=N] [--limit=N] [--shards=N] [--danger-threshold=N]"
                 " [--hot-tps=N] [--start-peak] [--component-id=N]"
                 " [--peak-reservation=N] [--cluster-reservation=component:capacity]..."
                 " [--udp-bind-port=N] [--udp-bind-host=IPv4] [--udp-peer=IPv4:port]..."
                 " [--handoff-bind-port=N] [--handoff-bind-host=IPv4]"
                 " [--handoff-leader=N] [--strict-owner=N]"
                 " [--handoff-peer=component:IPv4:port]..."
                 " [--handoff-auth-key-file=PATH] [--handoff-journal=PATH]"
                 " [--strict-owner-endpoint=IPv4:UDP_PORT]\n";
}

bool parse_redirect_endpoint(const std::string& text, RedirectEndpoint& endpoint) {
    const std::size_t separator = text.rfind(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 == text.size()) return false;
    std::uint16_t port = 0;
    if (!parse_number(std::string_view(text).substr(separator + 1), port) || port == 0) return false;
    in_addr address{};
    const std::string host = text.substr(0, separator);
    if (inet_pton(AF_INET, host.c_str(), &address) != 1) return false;
    endpoint = {ntohl(address.s_addr), port};
    return true;
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
    counter_poc::UdpCounterProtocol::Request request{};
    if (!counter_poc::UdpCounterProtocol::decode_request(
            std::span<const std::uint8_t>(payload, kWireSize), request))
        return false;
    const counter_poc::Result result =
        context.engine->apply({request.counter, request.delta, request.request_id});

    reverse_ethernet(ethernet);
    const rte_be32_t source_ip = ip->src_addr;
    ip->src_addr = ip->dst_addr;
    ip->dst_addr = source_ip;
    const rte_be16_t source_port = udp->src_port;
    udp->src_port = udp->dst_port;
    udp->dst_port = source_port;
    const auto reply = counter_poc::UdpCounterProtocol::encode_reply(
        {request.request_id, result, request.counter, context.redirect.ipv4_host_order,
         context.redirect.port});
    std::copy(reply.begin(), reply.end(), payload);
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
    const char* const transport = std::getenv("COUNTER_TRANSPORT");
    if (transport != nullptr && std::string_view(transport) != "dpdk") {
        std::cerr << "counterd_dpdk requires COUNTER_TRANSPORT=dpdk (or no setting)\n";
        return 2;
    }
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

    RedirectEndpoint redirect{};
    if (!options.strict_owner_endpoint.empty() &&
        !parse_redirect_endpoint(options.strict_owner_endpoint, redirect)) {
        std::cerr << "strict owner endpoint must be IPv4:UDP_PORT\n";
        rte_eth_dev_stop(options.dpdk_port);
        rte_eth_dev_close(options.dpdk_port);
        rte_eal_cleanup();
        return 2;
    }
    std::unique_ptr<counter_poc::CounterRuntime> runtime;
    try {
        runtime = std::make_unique<counter_poc::CounterRuntime>(
            counter_poc::make_runtime_config(options.runtime));
        runtime->start();
    } catch (const std::exception& error) {
        std::cerr << "cannot start DPDK counter runtime: " << error.what() << '\n';
        rte_eth_dev_stop(options.dpdk_port);
        rte_eth_dev_close(options.dpdk_port);
        rte_eal_cleanup();
        return 2;
    }
    std::signal(SIGINT, stop_handler);
    std::signal(SIGTERM, stop_handler);

    std::vector<WorkerContext> contexts;
    contexts.reserve(options.queues);
    for (std::uint16_t queue = 0; queue < options.queues; ++queue)
        contexts.push_back({options.dpdk_port, queue, options.udp_port, &runtime->engine(), redirect});

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
    runtime->stop();
    rte_eth_dev_stop(options.dpdk_port);
    rte_eth_dev_close(options.dpdk_port);
    rte_eal_cleanup();
    return 0;
}
