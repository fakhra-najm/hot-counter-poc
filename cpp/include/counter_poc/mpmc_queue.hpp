#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
namespace counter_poc {
// Bounded Vyukov MPMC queue. Full queues drop samples rather than blocking data-plane work.
template <typename T, std::size_t Capacity>
class MpmcQueue final {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    struct Cell { std::atomic<std::size_t> sequence; T value; };
public:
    MpmcQueue() noexcept { for (std::size_t i=0;i<Capacity;++i) cells_[i].sequence.store(i, std::memory_order_relaxed); }
    bool try_push(const T& value) noexcept {
        std::size_t pos = enqueue_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell=cells_[pos&(Capacity-1)]; std::size_t seq=cell.sequence.load(std::memory_order_acquire);
            const auto dif=static_cast<std::intptr_t>(seq)-static_cast<std::intptr_t>(pos);
            if (dif==0) { if (enqueue_.compare_exchange_weak(pos,pos+1,std::memory_order_relaxed)) { cell.value=value; cell.sequence.store(pos+1,std::memory_order_release); return true; } }
            else if (dif<0) return false; else pos=enqueue_.load(std::memory_order_relaxed);
        }
    }
    bool try_pop(T& value) noexcept {
        std::size_t pos = dequeue_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell=cells_[pos&(Capacity-1)]; std::size_t seq=cell.sequence.load(std::memory_order_acquire);
            const auto dif=static_cast<std::intptr_t>(seq)-static_cast<std::intptr_t>(pos+1);
            if (dif==0) { if (dequeue_.compare_exchange_weak(pos,pos+1,std::memory_order_relaxed)) { value=cell.value; cell.sequence.store(pos+Capacity,std::memory_order_release); return true; } }
            else if (dif<0) return false; else pos=dequeue_.load(std::memory_order_relaxed);
        }
    }
private:
    alignas(64) std::array<Cell, Capacity> cells_{};
    alignas(64) std::atomic<std::size_t> enqueue_{0};
    alignas(64) std::atomic<std::size_t> dequeue_{0};
};
}  // namespace counter_poc
