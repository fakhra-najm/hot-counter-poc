#pragma once
#include <string_view>
namespace counter_poc {
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual void run(std::string_view bind_address, unsigned short port) = 0;
};
// The DPDK implementation is compiled only with COUNTER_POC_WITH_DPDK and a validated ENA secondary ENI.
bool dpdk_transport_available() noexcept;
}  // namespace counter_poc
