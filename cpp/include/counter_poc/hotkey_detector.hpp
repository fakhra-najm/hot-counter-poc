#pragma once
#include "counter_poc/mpmc_queue.hpp"
#include "counter_poc/types.hpp"
#include <atomic>
#include <functional>
#include <thread>
namespace counter_poc {
struct Sample { CounterId counter; std::uint64_t timestamp_ns; };
class HotKeyDetector final {
public:
    using Callback = std::function<void(CounterId, std::uint64_t)>;
    HotKeyDetector(std::uint64_t threshold_tps, Callback on_hot);
    ~HotKeyDetector();
    bool record(CounterId counter, std::uint64_t timestamp_ns) noexcept;
    void start(); void stop();
    std::uint64_t dropped_samples() const noexcept { return dropped_.load(std::memory_order_relaxed); }
private:
    void run();
    MpmcQueue<Sample, 65536> queue_;
    const std::uint64_t threshold_tps_;
    Callback on_hot_;
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> dropped_{0};
    std::thread worker_;
};
}  // namespace counter_poc
