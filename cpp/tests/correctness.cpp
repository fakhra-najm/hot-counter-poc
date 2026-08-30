#include "counter_poc/engine.hpp"
#include "counter_poc/auth.hpp"
#include "counter_poc/distributed_handoff.hpp"
#include "counter_poc/gcounter.hpp"
#include "counter_poc/replication.hpp"
#include "counter_poc/reservation_plan.hpp"
#include "counter_poc/reserved_counter.hpp"
#include "counter_poc/runtime.hpp"
#include "counter_poc/runtime_config.hpp"
#include "counter_poc/strict_counter.hpp"
#include "counter_poc/udp_counter_protocol.hpp"
#include <chrono>
#include <cassert>
#include <iostream>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>
using namespace counter_poc;

namespace {
class RecordingLimitEventPublisher final : public ILimitReachedPublisher {
public:
    void publish(const LimitReachedEvent& event) noexcept override {
        count.fetch_add(1, std::memory_order_relaxed);
        last_limit.store(event.limit, std::memory_order_relaxed);
    }

    std::atomic<std::uint32_t> count{0};
    std::atomic<Amount> last_limit{0};
};

template <typename Predicate>
bool eventually(Predicate&& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return predicate();
}
}  // namespace

int main() {
    const std::vector<std::uint8_t> hmac_key(20, 0x0bU);
    const std::vector<std::uint8_t> hmac_message{'H', 'i', ' ', 'T', 'h', 'e', 'r', 'e'};
    const ControlAuthenticator::Tag expected_hmac = {
        0xb0U, 0x34U, 0x4cU, 0x61U, 0xd8U, 0xdbU, 0x38U, 0x53U,
        0x5cU, 0xa8U, 0xafU, 0xceU, 0xafU, 0x0bU, 0xf1U, 0x2bU};
    const ControlAuthenticator::Tag hmac = ControlAuthenticator::sign(hmac_key, hmac_message);
    assert(hmac == expected_hmac);
    assert(ControlAuthenticator::verify(hmac_key, hmac_message, hmac));
    std::vector<std::uint8_t> altered_message = hmac_message;
    altered_message[0] = 'h';
    assert(!ControlAuthenticator::verify(hmac_key, altered_message, hmac));

    const auto encoded_udp_request = UdpCounterProtocol::encode_request({9, 25, 17});
    UdpCounterProtocol::Request decoded_udp_request{};
    assert(UdpCounterProtocol::decode_request(encoded_udp_request, decoded_udp_request));
    assert(decoded_udp_request.request_id == 9 && decoded_udp_request.delta == 25 &&
           decoded_udp_request.counter == 17);
    const auto encoded_udp_reply = UdpCounterProtocol::encode_reply(
        {9, {Decision::Moved, 75, 0}, 17, 0x0a00000bU, 9091});
    UdpCounterProtocol::Reply decoded_udp_reply{};
    assert(UdpCounterProtocol::decode_reply(encoded_udp_reply, decoded_udp_reply));
    assert(decoded_udp_reply.result.decision == Decision::Moved &&
           decoded_udp_reply.result.observed == 75 &&
           decoded_udp_reply.strict_owner_ipv4 == 0x0a00000bU &&
           decoded_udp_reply.strict_owner_port == 9091);

    StrictCounter strict(100); assert(strict.apply(95).decision==Decision::Accepted);
    assert(strict.apply(6).decision==Decision::Rejected); assert(strict.value()==95);
    assert(strict.apply(5).decision==Decision::Accepted && strict.value()==100);
    StrictCounter raced(100000); std::vector<std::thread> threads;
    for(int n=0;n<8;++n) threads.emplace_back([&]{for(int i=0;i<100000;++i) raced.apply(3);});
    for(auto& thread:threads) thread.join();
    assert(raced.value()<=100000 && raced.value()%3==0);
    ReservedCounter reserved(100,2); assert(reserved.apply(0,50).decision==Decision::Accepted);
    assert(reserved.apply(0,1).decision==Decision::Rejected); assert(reserved.apply(1,50).decision==Decision::Accepted);
    assert(reserved.total()==100);
    ReservationPlan plan(100, {{0, 45}, {1, 55}});
    assert(plan.allocated_total() == 100 && plan.capacity_for(0) == 45);
    RuntimeLaunchOptions shared_runtime_options{};
    shared_runtime_options.limit = 100;
    shared_runtime_options.shards = 1;
    shared_runtime_options.danger_threshold = 10;
    shared_runtime_options.hot_threshold_tps = 1000000;
    shared_runtime_options.component_id = 1;
    shared_runtime_options.start_peak = true;
    shared_runtime_options.cluster_reservations = {{0, 45}, {1, 55}};
    CounterRuntimeConfig shared_runtime_config = make_runtime_config(shared_runtime_options);
    assert(shared_runtime_config.engine.local_peak_reservation == 55);
    assert(shared_runtime_config.handoff == std::nullopt);
    CounterRuntime shared_runtime(std::move(shared_runtime_config));
    shared_runtime.start();
    assert(shared_runtime.engine().mode() == RoutingMode::ReservedPeak);
    assert(shared_runtime.engine().apply({1, 55, 0}).decision == Decision::Accepted);
    shared_runtime.stop();
    CounterEngine peak_a({100, 1, 10, 1000000, 0, plan.capacity_for(0)});
    CounterEngine peak_b({100, 1, 10, 1000000, 1, plan.capacity_for(1)});
    assert(peak_a.enter_reserved_mode_after_quiescence());
    assert(peak_b.enter_reserved_mode_after_quiescence());
    assert(peak_a.apply({1, 45, 0}).decision == Decision::Accepted);
    assert(peak_b.apply({1, 55, 1}).decision == Decision::Accepted);
    assert(peak_a.apply({1, 1, 0}).decision == Decision::Rejected);
    assert(peak_b.apply({1, 1, 1}).decision == Decision::Rejected);
    assert(peak_a.mode() == RoutingMode::ReservedPeak &&
           !peak_a.enter_danger_mode_after_quiescence());
    CounterEngine handoff_a({100, 1, 10, 1000000, 0, 45});
    CounterEngine handoff_b({100, 1, 10, 1000000, 1, 55});
    assert(handoff_a.enter_reserved_mode_after_quiescence());
    assert(handoff_b.enter_reserved_mode_after_quiescence());
    assert(handoff_a.apply({17, 40, 0}).decision == Decision::Accepted);
    assert(handoff_b.apply({17, 50, 0}).decision == Decision::Accepted);
    DrainedPeakState state_a{};
    DrainedPeakState state_b{};
    assert(handoff_a.begin_distributed_handoff(state_a));
    assert(handoff_b.begin_distributed_handoff(state_b));
    assert(state_a.component_total == 40 && state_b.component_total == 50);
    assert(handoff_a.commit_distributed_handoff(90, true));
    assert(handoff_b.commit_distributed_handoff(90, false));
    assert(handoff_a.mode() == RoutingMode::DangerStrict);
    assert(handoff_b.mode() == RoutingMode::RemoteStrict);
    assert(handoff_a.apply({17, 10, 0}).decision == Decision::Accepted);
    assert(handoff_a.apply({17, 1, 0}).decision == Decision::Rejected);
    assert(handoff_b.apply({17, 1, 0}).decision == Decision::Moved);

    CounterEngine recovered_owner({100, 1, 10, 1000000, 0, 45});
    assert(recovered_owner.enter_reserved_mode_after_quiescence());
    assert(recovered_owner.recover_committed_handoff({17, 4, 90}, 90, true));
    assert(recovered_owner.mode() == RoutingMode::DangerStrict);
    assert(recovered_owner.apply({17, 10, 0}).decision == Decision::Accepted);
    CounterEngine interrupted({100, 1, 10, 1000000, 0, 45});
    assert(interrupted.enter_reserved_mode_after_quiescence());
    interrupted.fence_after_interrupted_handoff();
    assert(interrupted.apply({17, 1, 0}).decision == Decision::Rejected);

    CounterEngine coordinated_a({100, 1, 10, 1000000, 0, 45});
    CounterEngine coordinated_b({100, 1, 10, 1000000, 1, 55});
    assert(coordinated_a.enter_reserved_mode_after_quiescence());
    assert(coordinated_b.enter_reserved_mode_after_quiescence());
    assert(coordinated_a.apply({19, 40, 0}).decision == Decision::Accepted);
    assert(coordinated_b.apply({19, 50, 0}).decision == Decision::Accepted);
    DistributedHandoffConfig controller_a_config{};
    controller_a_config.component_id = 0;
    controller_a_config.leader_component_id = 0;
    controller_a_config.strict_owner_component_id = 0;
    controller_a_config.bind_host = "127.0.0.1";
    controller_a_config.bind_port = 32101;
    controller_a_config.members = {0, 1};
    controller_a_config.peers = {{1, "127.0.0.1", 32102}};
    controller_a_config.authentication_key = std::vector<std::uint8_t>(32, 0xa5U);
    controller_a_config.journal_path = "/tmp/counter-poc-handoff-a-" + std::to_string(getpid());
    controller_a_config.retry_after = std::chrono::milliseconds(2);
    controller_a_config.prepare_timeout = std::chrono::milliseconds(500);
    DistributedHandoffConfig controller_b_config{};
    controller_b_config.component_id = 1;
    controller_b_config.leader_component_id = 0;
    controller_b_config.strict_owner_component_id = 0;
    controller_b_config.bind_host = "127.0.0.1";
    controller_b_config.bind_port = 32102;
    controller_b_config.members = {0, 1};
    controller_b_config.peers = {{0, "127.0.0.1", 32101}};
    controller_b_config.authentication_key = controller_a_config.authentication_key;
    controller_b_config.journal_path = "/tmp/counter-poc-handoff-b-" + std::to_string(getpid());
    controller_b_config.retry_after = std::chrono::milliseconds(2);
    controller_b_config.prepare_timeout = std::chrono::milliseconds(500);
    try {
        DistributedHandoffController controller_a(coordinated_a, std::move(controller_a_config));
        DistributedHandoffController controller_b(coordinated_b, std::move(controller_b_config));
        controller_a.start();
        controller_b.start();
        assert(eventually([&] {
            return coordinated_a.mode() == RoutingMode::DangerStrict &&
                   coordinated_b.mode() == RoutingMode::RemoteStrict;
        }));
        assert(controller_a.stats().commits >= 1);
        assert(coordinated_a.apply({19, 10, 0}).decision == Decision::Accepted);
        assert(coordinated_b.apply({19, 1, 0}).decision == Decision::Moved);
    } catch (const std::system_error& error) {
        assert(error.code() == std::errc::operation_not_permitted ||
               error.code() == std::errc::permission_denied);
        std::cerr << "distributed handoff integration skipped: " << error.what() << '\n';
    }

    CounterRuntimeConfig runtime_handoff_a_config{{100, 1, 10, 1000000, 0, 45}, true,
                                                  std::nullopt, std::nullopt};
    CounterRuntimeConfig runtime_handoff_b_config{{100, 1, 10, 1000000, 1, 55}, true,
                                                  std::nullopt, std::nullopt};
    DistributedHandoffConfig runtime_handoff_a_control{};
    runtime_handoff_a_control.component_id = 0;
    runtime_handoff_a_control.leader_component_id = 0;
    runtime_handoff_a_control.strict_owner_component_id = 0;
    runtime_handoff_a_control.bind_host = "127.0.0.1";
    runtime_handoff_a_control.bind_port = 32105;
    runtime_handoff_a_control.members = {0, 1};
    runtime_handoff_a_control.peers = {{1, "127.0.0.1", 32106}};
    runtime_handoff_a_control.authentication_key = std::vector<std::uint8_t>(32, 0x3cU);
    runtime_handoff_a_control.journal_path =
        "/tmp/counter-poc-runtime-handoff-a-" + std::to_string(getpid());
    runtime_handoff_a_control.retry_after = std::chrono::milliseconds(2);
    runtime_handoff_a_control.prepare_timeout = std::chrono::milliseconds(500);
    DistributedHandoffConfig runtime_handoff_b_control{};
    runtime_handoff_b_control.component_id = 1;
    runtime_handoff_b_control.leader_component_id = 0;
    runtime_handoff_b_control.strict_owner_component_id = 0;
    runtime_handoff_b_control.bind_host = "127.0.0.1";
    runtime_handoff_b_control.bind_port = 32106;
    runtime_handoff_b_control.members = {0, 1};
    runtime_handoff_b_control.peers = {{0, "127.0.0.1", 32105}};
    runtime_handoff_b_control.authentication_key = runtime_handoff_a_control.authentication_key;
    runtime_handoff_b_control.journal_path =
        "/tmp/counter-poc-runtime-handoff-b-" + std::to_string(getpid());
    runtime_handoff_b_control.retry_after = std::chrono::milliseconds(2);
    runtime_handoff_b_control.prepare_timeout = std::chrono::milliseconds(500);
    runtime_handoff_a_config.handoff = std::move(runtime_handoff_a_control);
    runtime_handoff_b_config.handoff = std::move(runtime_handoff_b_control);
    try {
        CounterRuntime runtime_handoff_a(std::move(runtime_handoff_a_config));
        CounterRuntime runtime_handoff_b(std::move(runtime_handoff_b_config));
        runtime_handoff_a.start();
        runtime_handoff_b.start();
        assert(runtime_handoff_a.engine().apply({29, 40, 0}).decision == Decision::Accepted);
        assert(runtime_handoff_b.engine().apply({29, 50, 0}).decision == Decision::Accepted);
        assert(eventually([&] {
            return runtime_handoff_a.engine().mode() == RoutingMode::DangerStrict &&
                   runtime_handoff_b.engine().mode() == RoutingMode::RemoteStrict;
        }));
    } catch (const std::system_error& error) {
        assert(error.code() == std::errc::operation_not_permitted ||
               error.code() == std::errc::permission_denied);
        std::cerr << "shared runtime handoff integration skipped: " << error.what() << '\n';
    }

    CounterEngine timeout_a({100, 1, 10, 1000000, 0, 45});
    assert(timeout_a.enter_reserved_mode_after_quiescence());
    assert(timeout_a.apply({23, 40, 0}).decision == Decision::Accepted);
    DistributedHandoffConfig timeout_config{};
    timeout_config.component_id = 0;
    timeout_config.leader_component_id = 0;
    timeout_config.strict_owner_component_id = 0;
    timeout_config.bind_host = "127.0.0.1";
    timeout_config.bind_port = 32103;
    timeout_config.members = {0, 1};
    timeout_config.peers = {{1, "127.0.0.1", 32104}};
    timeout_config.authentication_key = std::vector<std::uint8_t>(32, 0x5aU);
    timeout_config.journal_path = "/tmp/counter-poc-handoff-timeout-" + std::to_string(getpid());
    timeout_config.retry_after = std::chrono::milliseconds(2);
    timeout_config.prepare_timeout = std::chrono::milliseconds(25);
    try {
        DistributedHandoffController timeout_controller(timeout_a, std::move(timeout_config));
        timeout_controller.start();
        assert(eventually([&] { return timeout_controller.stats().aborts >= 1; }));
        timeout_controller.stop();
        assert(timeout_a.mode() == RoutingMode::ReservedPeak);
        assert(timeout_a.apply({23, 1, 0}).decision == Decision::Accepted);
    } catch (const std::system_error& error) {
        assert(error.code() == std::errc::operation_not_permitted ||
               error.code() == std::errc::permission_denied);
        std::cerr << "distributed handoff timeout skipped: " << error.what() << '\n';
    }
    GCounter a(2),b(2); a.increment(0,8); b.increment(1,8); a.merge(b); b.merge(a);
    assert(a.total()==16 && b.total()==16);
    CounterEngine engine(100,2,10,1); assert(engine.apply({1,30}).decision==Decision::Accepted);
    assert(engine.enter_reserved_mode_after_quiescence()); assert(engine.mode()==RoutingMode::ReservedPeak);
    assert(engine.enter_danger_mode_after_quiescence()); assert(engine.apply({1,70}).decision==Decision::Accepted);
    CounterEngine danger_engine(100, 1, 10, 1000000);
    assert(danger_engine.enter_reserved_mode_after_quiescence());
    danger_engine.start_control_plane();
    assert(danger_engine.apply({1, 90, 0}).decision == Decision::Accepted);
    assert(eventually([&] { return danger_engine.danger_transition_requested() ||
                                   danger_engine.mode() == RoutingMode::DangerStrict; }));
    danger_engine.stop_control_plane();

    RecordingLimitEventPublisher limit_events;
    CounterEngine exhausted_engine(100, 1, 10, 1000000);
    exhausted_engine.set_limit_reached_publisher(&limit_events);
    assert(exhausted_engine.apply({9, 95, 0}).decision == Decision::Accepted);
    assert(exhausted_engine.apply({9, 10, 0}).decision == Decision::Rejected);
    assert(limit_events.count.load(std::memory_order_relaxed) == 0);
    assert(exhausted_engine.apply({9, 5, 0}).decision == Decision::Accepted);
    assert(exhausted_engine.limit_exhausted());
    assert(limit_events.count.load(std::memory_order_relaxed) == 1);
    assert(limit_events.last_limit.load(std::memory_order_relaxed) == 100);
    assert(exhausted_engine.apply({9, 1, 0}).decision == Decision::Rejected);
    assert(limit_events.count.load(std::memory_order_relaxed) == 1);

    RecordingLimitEventPublisher peak_limit_events;
    CounterEngine peak_exhausted_engine(100, 2, 10, 1000000);
    peak_exhausted_engine.set_limit_reached_publisher(&peak_limit_events);
    assert(peak_exhausted_engine.enter_reserved_mode_after_quiescence());
    assert(peak_exhausted_engine.apply({11, 50, 0}).decision == Decision::Accepted);
    assert(peak_limit_events.count.load(std::memory_order_relaxed) == 0);
    assert(peak_exhausted_engine.apply({11, 50, 1}).decision == Decision::Accepted);
    assert(peak_exhausted_engine.limit_exhausted());
    assert(peak_limit_events.count.load(std::memory_order_relaxed) == 1);
    assert(peak_exhausted_engine.apply({11, 1, 0}).decision == Decision::Rejected);

    CounterEngine automatic_engine(1000, 1, 100, 1);
    automatic_engine.start_control_plane();
    assert(automatic_engine.apply({1, 1, 0}).decision == Decision::Accepted);
    assert(eventually([&] { return automatic_engine.mode() == RoutingMode::ReservedPeak; }));
    assert(automatic_engine.apply({1, 900, 0}).decision == Decision::Accepted);
    assert(eventually([&] { return automatic_engine.mode() == RoutingMode::DangerStrict; }));
    automatic_engine.stop_control_plane();

    UdpReplicationConfig receiver_config{};
    receiver_config.component_id = 1;
    receiver_config.bind_port = 31992;
    receiver_config.retry_after = std::chrono::milliseconds(2);
    receiver_config.max_retries = 4;
    receiver_config.bind_host = "127.0.0.1";
    UdpReplicationConfig sender_config{};
    sender_config.bind_port = 31991;
    sender_config.peers = {{"127.0.0.1", 31992}};
    sender_config.retry_after = std::chrono::milliseconds(2);
    sender_config.max_retries = 4;
    sender_config.bind_host = "127.0.0.1";
    ReliableUdpReplicator receiver(std::move(receiver_config));
    ReliableUdpReplicator sender(std::move(sender_config));
    try {
        receiver.start();
        sender.start();
        sender.publish({42, 7, 0, 500});
        assert(eventually([&] { return receiver.component_value(42, 7, 0) == 500; }));
        assert(receiver.merged_total(42, 7) == 500);
        assert(eventually([&] { return sender.stats().acknowledged >= 1; }));
        sender.stop();
        receiver.stop();
    } catch (const std::system_error& error) {
        // Some desktop sandboxes explicitly forbid binding sockets. Linux CI
        // and an EC2 host run this integration assertion; this environment
        // still compiles every transport path and runs the deterministic tests.
        assert(error.code() == std::errc::operation_not_permitted ||
               error.code() == std::errc::permission_denied);
        std::cerr << "UDP integration skipped: " << error.what() << '\n';
    }
}
