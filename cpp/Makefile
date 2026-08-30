CXX ?= c++
CXXFLAGS = -O3 -std=c++20 -Wall -Wextra -Werror -pthread -Iinclude
BUILD = build
CORE = src/strict_counter.cpp src/reserved_counter.cpp src/reservation_plan.cpp src/routing.cpp src/gcounter.cpp src/hotkey_detector.cpp src/engine.cpp src/replication.cpp src/auth.cpp src/distributed_handoff.cpp src/runtime_config.cpp src/runtime.cpp src/udp_counter_protocol.cpp src/transport.cpp
TCP = src/tcp_server.cpp
all: $(BUILD)/counterd $(BUILD)/correctness $(BUILD)/counter_bench
$(BUILD):
	mkdir -p $@
$(BUILD)/counterd: $(CORE) $(TCP) server/main.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $^ -o $@
$(BUILD)/correctness: $(CORE) tests/correctness.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $^ -o $@
$(BUILD)/counter_bench: $(CORE) benchmark/main.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $^ -o $@
test: $(BUILD)/correctness
	$(BUILD)/correctness
dpdk: $(BUILD)
	@pkg-config --exists libdpdk || (echo "libdpdk development files are required"; exit 2)
	$(CXX) $(CXXFLAGS) -DCOUNTER_POC_WITH_DPDK $$(pkg-config --cflags libdpdk) $(CORE) dpdk/main.cpp -o $(BUILD)/counterd_dpdk $$(pkg-config --libs libdpdk)
dpdk-client: $(BUILD)
	$(CXX) $(CXXFLAGS) src/udp_counter_protocol.cpp dpdk/udp_client.cpp -o $(BUILD)/counterd_udp_client
clean:
	rm -rf $(BUILD)
.PHONY: all test dpdk dpdk-client clean
