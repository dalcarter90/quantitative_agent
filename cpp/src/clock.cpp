#include "qa/clock.hpp"

#include <chrono>

namespace qa {

Timestamp SystemClock::wall() const noexcept {
  const auto since_epoch = std::chrono::system_clock::now().time_since_epoch();
  return Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch).count());
}

MonoTimestamp SystemClock::mono() const noexcept {
  const auto since_boot = std::chrono::steady_clock::now().time_since_epoch();
  return MonoTimestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(since_boot).count());
}

}  // namespace qa
