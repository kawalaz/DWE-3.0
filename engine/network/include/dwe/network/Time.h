#pragma once

#include <chrono>

namespace dwe::network {

// One clock for the whole network layer. Time is always passed in explicitly so that the
// protocol state machines can be driven by a virtual clock in deterministic tests and in the
// in-process simulator.
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

} // namespace dwe::network
