#include "counter_poc/engine.hpp"

#include <barrier>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using counter_poc::Amount;

enum class BenchmarkMode : std::uint8_t { Strict, Peak, Danger };

struct Options {
    BenchmarkMode mode{BenchmarkMode::Strict};
    std::uint32_t threads{1};
    std::uint32_t shards{1};
    std::uint64_t duration_ms{1000};
    Amount limit{1000000000000000ULL};
    Amount minimum_delta{1};
    Amount maximum_delta{1};
};

struct ThreadResult {
    std::uint64_t attempted{};
    std::uint64_t accepted{};
    std::uint64_t rejected{};
    std::uint64_t moved{};
};

template <typename Number>
bool parse_number(std::string_view text, Number& output) noexcept {
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), output);
    return error == std::errc{} && end == text.data() + text.size();
}

bool parse_options(int argc, char** argv, Options& options) {
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        const auto value_after = [&argument](std::string_view name) -> std::string_view {
            return argument.starts_with(name) ? argument.substr(name.size()) : std::string_view{};
        };
        if (const std::string_view value = value_after("--mode="); !value.empty()) {
            if (value == "strict") options.mode = BenchmarkMode::Strict;
            else if (value == "peak") options.mode = BenchmarkMode::Peak;
            else if (value == "danger") options.mode = BenchmarkMode::Danger;
            else return false;
        } else if (const std::string_view value = value_after("--threads="); !value.empty()) {
            if (!parse_number(value, options.threads)) return false;
        } else if (const std::string_view value = value_after("--shards="); !value.empty()) {
            if (!parse_number(value, options.shards)) return false;
        } else if (const std::string_view value = value_after("--duration-ms="); !value.empty()) {
            if (!parse_number(value, options.duration_ms)) return false;
        } else if (const std::string_view value = value_after("--limit="); !value.empty()) {
            if (!parse_number(value, options.limit)) return false;
        } else if (const std::string_view value = value_after("--min-delta="); !value.empty()) {
            if (!parse_number(value, options.minimum_delta)) return false;
        } else if (const std::string_view value = value_after("--max-delta="); !value.empty()) {
            if (!parse_number(value, options.maximum_delta)) return false;
        } else {
            return false;
        }
    }
    return options.threads != 0 && options.shards != 0 && options.duration_ms != 0 &&
           options.limit != 0 && options.minimum_delta != 0 &&
           options.minimum_delta <= options.maximum_delta && options.maximum_delta <= options.limit;
}

const char* mode_name(BenchmarkMode mode) noexcept {
    switch (mode) {
        case BenchmarkMode::Strict: return "strict";
        case BenchmarkMode::Peak: return "peak";
        case BenchmarkMode::Danger: return "danger";
    }
    return "unknown";
}

std::uint64_t next_random(std::uint64_t& state) noexcept {
    // xorshift64* is local to one worker: it introduces no cross-thread
    // benchmark coordination and produces a repeatable delta distribution.
    state ^= state >> 12U;
    state ^= state << 25U;
    state ^= state >> 27U;
    return state * 2685821657736338717ULL;
}

Amount next_delta(std::uint64_t& random, const Options& options) noexcept {
    const Amount span = options.maximum_delta - options.minimum_delta;
    if (span == 0) return options.minimum_delta;
    return options.minimum_delta + next_random(random) % (span + 1);
}

void print_usage() {
    std::cerr << "usage: counter_bench [--mode=strict|peak|danger] [--threads=N] [--shards=N]"
                 " [--duration-ms=N] [--limit=N] [--min-delta=N] [--max-delta=N]\n";
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    if (!parse_options(argc, argv, options)) {
        print_usage();
        return 2;
    }

    counter_poc::CounterEngine engine(
        {options.limit, options.shards, 0, std::numeric_limits<std::uint64_t>::max(), 0, 0});
    if (options.mode != BenchmarkMode::Strict && !engine.enter_reserved_mode_after_quiescence()) {
        std::cerr << "cannot enter peak mode\n";
        return 2;
    }
    if (options.mode == BenchmarkMode::Danger && !engine.enter_danger_mode_after_quiescence()) {
        std::cerr << "cannot enter danger mode\n";
        return 2;
    }

    std::vector<ThreadResult> results(options.threads);
    std::barrier start_line(static_cast<std::ptrdiff_t>(options.threads + 1));
    std::vector<std::thread> workers;
    workers.reserve(options.threads);
    for (std::uint32_t worker = 0; worker < options.threads; ++worker) {
        workers.emplace_back([&, worker] {
            std::uint64_t random = 0x9e3779b97f4a7c15ULL ^ (static_cast<std::uint64_t>(worker) + 1U);
            std::uint64_t sequence = 0;
            ThreadResult local{};
            start_line.arrive_and_wait();
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(options.duration_ms);
            while (std::chrono::steady_clock::now() < deadline) {
                const counter_poc::Result result = engine.apply(
                    {0, next_delta(random, options), (static_cast<std::uint64_t>(worker) << 32U) | sequence++});
                ++local.attempted;
                if (result.decision == counter_poc::Decision::Accepted) ++local.accepted;
                else if (result.decision == counter_poc::Decision::Moved) ++local.moved;
                else ++local.rejected;
            }
            results[worker] = local;
        });
    }

    start_line.arrive_and_wait();
    const auto started = std::chrono::steady_clock::now();
    for (std::thread& worker : workers) worker.join();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - started).count();

    ThreadResult total{};
    for (const ThreadResult& result : results) {
        total.attempted += result.attempted;
        total.accepted += result.accepted;
        total.rejected += result.rejected;
        total.moved += result.moved;
    }
    const double seconds = static_cast<double>(elapsed) / 1'000'000'000.0;
    const double operations_per_second = seconds == 0.0 ? 0.0 :
                                         static_cast<double>(total.attempted) / seconds;
    const double mean_nanoseconds = total.attempted == 0 ? 0.0 :
                                    static_cast<double>(elapsed) / total.attempted;
    std::cout << "mode,threads,shards,duration_ns,min_delta,max_delta,attempted,accepted,rejected,moved,"
                 "attempted_ops_per_second,mean_ns_per_attempt,final_value\n";
    std::cout << mode_name(options.mode) << ',' << options.threads << ',' << options.shards << ','
              << elapsed << ',' << options.minimum_delta << ',' << options.maximum_delta << ','
              << total.attempted << ',' << total.accepted << ',' << total.rejected << ',' << total.moved
              << ',' << operations_per_second << ',' << mean_nanoseconds << ','
              << engine.current_value()
              << '\n';
}
