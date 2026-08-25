#include "counter_poc/transport.hpp"
namespace counter_poc {
#ifdef COUNTER_POC_WITH_DPDK
bool dpdk_transport_available() noexcept { return true; }
#else
bool dpdk_transport_available() noexcept { return false; }
#endif
}  // namespace counter_poc
