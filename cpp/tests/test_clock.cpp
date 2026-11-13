// The Clock interface (architecture section 5: hot-path code never reads the
// system clock directly; time comes from an injected Clock).

#include "qa/clock.hpp"

#include <gtest/gtest.h>

#include <chrono>

namespace {

TEST(Clock, ManualClockReturnsExactlyWhatItIsSetTo) {
  qa::ManualClock clock(qa::Timestamp(1'000), qa::MonoTimestamp(50));
  const qa::Clock& as_interface = clock;
  EXPECT_EQ(as_interface.wall(), qa::Timestamp(1'000));
  EXPECT_EQ(as_interface.mono(), qa::MonoTimestamp(50));
  clock.advance(25);
  EXPECT_EQ(as_interface.wall(), qa::Timestamp(1'025));
  EXPECT_EQ(as_interface.mono(), qa::MonoTimestamp(75));
  clock.set(qa::Timestamp(7), qa::MonoTimestamp(9));  // the wall clock may step back
  EXPECT_EQ(as_interface.wall(), qa::Timestamp(7));
  EXPECT_EQ(as_interface.mono(), qa::MonoTimestamp(9));
}

TEST(Clock, ManualClockStepsTheWallClockAlone) {
  qa::ManualClock clock(qa::Timestamp(1'000), qa::MonoTimestamp(50));
  clock.step_wall(-1'241'000'000);
  EXPECT_EQ(clock.wall(), qa::Timestamp(1'000 - 1'241'000'000));
  EXPECT_EQ(clock.mono(), qa::MonoTimestamp(50));
}

TEST(Clock, SystemClockReadsTheWallClockAndAMonotonicCounter) {
  const qa::SystemClock clock;
  const auto before = std::chrono::system_clock::now().time_since_epoch();
  const qa::Timestamp wall = clock.wall();
  const auto after = std::chrono::system_clock::now().time_since_epoch();
  EXPECT_GE(wall.ns(), std::chrono::duration_cast<std::chrono::nanoseconds>(before).count());
  EXPECT_LE(wall.ns(), std::chrono::duration_cast<std::chrono::nanoseconds>(after).count());
  EXPECT_GT(wall.ns(), 1'600'000'000'000'000'000);  // after 2020: a Unix-epoch time

  qa::MonoTimestamp previous = clock.mono();
  for (int i = 0; i < 10'000; ++i) {
    const qa::MonoTimestamp now = clock.mono();
    ASSERT_GE(now, previous);
    previous = now;
  }
}

}  // namespace
