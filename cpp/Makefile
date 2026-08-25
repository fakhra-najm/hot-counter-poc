CXX ?= c++
CXXFLAGS = -O3 -std=c++20 -Wall -Wextra -Werror -pthread -Iinclude
BUILD = build
CORE = src/strict_counter.cpp src/reserved_counter.cpp src/routing.cpp src/gcounter.cpp src/hotkey_detector.cpp src/engine.cpp src/transport.cpp
all: $(BUILD)/counterd $(BUILD)/correctness
$(BUILD):
	mkdir -p $@
$(BUILD)/counterd: $(CORE) server/main.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $^ -o $@
$(BUILD)/correctness: $(CORE) tests/correctness.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $^ -o $@
test: $(BUILD)/correctness
	$(BUILD)/correctness
clean:
	rm -rf $(BUILD)
.PHONY: all test clean
