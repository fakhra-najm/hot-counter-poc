#pragma once
#include "counter_poc/types.hpp"
namespace counter_poc {
class ICounter {
public:
    virtual ~ICounter() = default;
    virtual Result apply(Amount delta) noexcept = 0;
    virtual Amount value() const noexcept = 0;
    virtual Amount limit() const noexcept = 0;
};
}  // namespace counter_poc
