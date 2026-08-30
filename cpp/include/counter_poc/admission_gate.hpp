#pragma once

#include <atomic>
#include <cstdint>

namespace counter_poc {

// Closing the gate prevents new work from entering. Callers that entered before
// close() are counted, allowing a control-plane transition to wait for a real
// quiescent point before moving exact counter state.
class AdmissionGate final {
public:
    bool try_enter() noexcept {
        if (!open_.load(std::memory_order_acquire)) return false;
        in_flight_.fetch_add(1, std::memory_order_acq_rel);
        if (open_.load(std::memory_order_acquire)) return true;
        in_flight_.fetch_sub(1, std::memory_order_release);
        return false;
    }

    void leave() noexcept { in_flight_.fetch_sub(1, std::memory_order_release); }
    void close() noexcept { open_.store(false, std::memory_order_release); }
    void open() noexcept { open_.store(true, std::memory_order_release); }
    bool drained() const noexcept { return in_flight_.load(std::memory_order_acquire) == 0; }
    bool accepting() const noexcept { return open_.load(std::memory_order_acquire); }

private:
    std::atomic<bool> open_{true};
    alignas(64) std::atomic<std::uint32_t> in_flight_{0};
};

}  // namespace counter_poc
