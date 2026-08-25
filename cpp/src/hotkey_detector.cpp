#include "counter_poc/hotkey_detector.hpp"
#include <chrono>
#include <unordered_map>
namespace counter_poc {
HotKeyDetector::HotKeyDetector(std::uint64_t threshold_tps, Callback on_hot) : threshold_tps_(threshold_tps), on_hot_(std::move(on_hot)) {}
HotKeyDetector::~HotKeyDetector() { stop(); }
bool HotKeyDetector::record(CounterId counter, std::uint64_t timestamp_ns) noexcept {
    if (queue_.try_push({counter,timestamp_ns})) return true;
    dropped_.fetch_add(1,std::memory_order_relaxed); return false;
}
void HotKeyDetector::start() { if (!worker_.joinable()) worker_=std::thread(&HotKeyDetector::run,this); }
void HotKeyDetector::stop() { stop_.store(true,std::memory_order_release); if (worker_.joinable()) worker_.join(); }
void HotKeyDetector::run() {
    std::unordered_map<CounterId,std::uint64_t> counts;
    while (!stop_.load(std::memory_order_acquire)) {
        Sample sample; while (queue_.try_pop(sample)) ++counts[sample.counter];
        for (const auto& [counter,count]:counts) if (count>=threshold_tps_/10) on_hot_(counter,count*10);
        counts.clear(); std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}
}  // namespace counter_poc
