#include "counter_poc/engine.hpp"
#include "counter_poc/gcounter.hpp"
#include "counter_poc/reserved_counter.hpp"
#include "counter_poc/strict_counter.hpp"
#include <cassert>
#include <thread>
#include <vector>
using namespace counter_poc;
int main() {
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
    GCounter a(2),b(2); a.increment(0,8); b.increment(1,8); a.merge(b); b.merge(a);
    assert(a.total()==16 && b.total()==16);
    CounterEngine engine(100,2,10,1); assert(engine.apply({1,30}).decision==Decision::Accepted);
    assert(engine.enter_reserved_mode_after_quiescence()); assert(engine.mode()==RoutingMode::ReservedPeak);
    assert(engine.enter_danger_mode_after_quiescence()); assert(engine.apply({1,70}).decision==Decision::Accepted);
    CounterEngine danger_engine(100, 1, 10, 1000000);
    assert(danger_engine.enter_reserved_mode_after_quiescence());
    assert(danger_engine.apply({1, 90, 0}).decision == Decision::Accepted);
    assert(danger_engine.danger_transition_requested());
}
