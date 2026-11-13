#pragma once

// Where hot-path code gets the time (architecture section 5: no component on
// the core thread reads the system clock directly).
//
// A Clock gives both times the recorder stamps (ADR 0012): the wall clock,
// which a time service can step, and the monotonic counter, which it cannot.
// SystemClock reads the same counter as Python's time.perf_counter_ns, so
// stamps taken in C++ and in the Python recorder are comparable.

#include "qa/types.hpp"

#include <cstdint>

namespace qa {

class Clock {
 public:
  virtual ~Clock() = default;
  [[nodiscard]] virtual Timestamp wall() const noexcept = 0;
  [[nodiscard]] virtual MonoTimestamp mono() const noexcept = 0;
};

// std::chrono::system_clock and steady_clock (QueryPerformanceCounter on
// Windows, CLOCK_MONOTONIC on Linux).
class SystemClock final : public Clock {
 public:
  [[nodiscard]] Timestamp wall() const noexcept override;
  [[nodiscard]] MonoTimestamp mono() const noexcept override;
};

// Returns exactly what it is set to. For replay and tests.
class ManualClock final : public Clock {
 public:
  constexpr ManualClock(Timestamp wall, MonoTimestamp mono) noexcept : wall_(wall), mono_(mono) {}

  [[nodiscard]] Timestamp wall() const noexcept override { return wall_; }
  [[nodiscard]] MonoTimestamp mono() const noexcept override { return mono_; }

  void set(Timestamp wall, MonoTimestamp mono) noexcept {
    wall_ = wall;
    mono_ = mono;
  }
  // Both clocks move forward by `ns`.
  void advance(std::int64_t ns) noexcept {
    wall_ = Timestamp(wall_.ns() + ns);
    mono_ = MonoTimestamp(mono_.ns() + ns);
  }
  // The wall clock alone jumps, as a time service stepping it would.
  void step_wall(std::int64_t ns) noexcept { wall_ = Timestamp(wall_.ns() + ns); }

 private:
  Timestamp wall_;
  MonoTimestamp mono_;
};

}  // namespace qa
