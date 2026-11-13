// Notional = price mantissa x quantity mantissa, which can exceed 64 bits
// (ADR 0005). The product is always computed exactly in 128 bits.

#include "qa/notional.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>

namespace {

using qa::Int128;

constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
constexpr auto kMin = std::numeric_limits<std::int64_t>::min();

TEST(Notional, SmallProductsFitInInt64) {
  const Int128 product = qa::notional(qa::Price(734685), qa::Qty(20416909));
  std::int64_t value = 0;
  ASSERT_TRUE(qa::to_int64(product, value));
  EXPECT_EQ(value, std::int64_t{734685} * 20416909);
}

TEST(Notional, KnownWideProducts) {
  // (2^63 - 1)^2 = 2^126 - 2^64 + 1
  EXPECT_EQ(qa::multiply(kMax, kMax), (Int128{0x3FFFFFFFFFFFFFFF, 0x0000000000000001}));
  // (-2^63)^2 = 2^126
  EXPECT_EQ(qa::multiply(kMin, kMin), (Int128{0x4000000000000000, 0}));
  // -2^63 * (2^63 - 1) = -2^126 + 2^63
  EXPECT_EQ(qa::multiply(kMin, kMax),
            (Int128{static_cast<std::int64_t>(0xC000000000000000), 0x8000000000000000}));
  // -1 * 1 = -1: all ones
  EXPECT_EQ(qa::multiply(-1, 1), (Int128{-1, 0xFFFFFFFFFFFFFFFF}));
  EXPECT_EQ(qa::multiply(0, kMin), (Int128{0, 0}));
  // 2^32 * 2^32 = 2^64
  EXPECT_EQ(qa::multiply(std::int64_t{1} << 32, std::int64_t{1} << 32), (Int128{1, 0}));
}

TEST(Notional, OverflowIsDetectedExactlyAtTheInt64Boundary) {
  std::int64_t value = 0;
  // 3037000499^2 < 2^63 - 1 < 3037000500^2
  EXPECT_TRUE(qa::to_int64(qa::multiply(3037000499, 3037000499), value));
  EXPECT_EQ(value, std::int64_t{3037000499} * 3037000499);
  EXPECT_FALSE(qa::to_int64(qa::multiply(3037000500, 3037000500), value));
  EXPECT_FALSE(qa::to_int64(qa::multiply(-3037000500, 3037000500), value));
  // The boundaries themselves.
  EXPECT_TRUE(qa::to_int64(qa::multiply(kMax, 1), value));
  EXPECT_EQ(value, kMax);
  EXPECT_TRUE(qa::to_int64(qa::multiply(kMin, 1), value));
  EXPECT_EQ(value, kMin);
  EXPECT_FALSE(qa::to_int64(qa::multiply(kMin, -1), value));  // 2^63
  EXPECT_FALSE(qa::to_int64(qa::multiply(kMax, 2), value));
  EXPECT_TRUE(qa::to_int64(qa::multiply(-(std::int64_t{1} << 62), 2), value));
  EXPECT_EQ(value, kMin);
}

TEST(Notional, ThePortableMultiplyAgreesWithThePlatformOne) {
  std::mt19937_64 random(20261112);
  std::uniform_int_distribution<std::int64_t> any(kMin, kMax);
  std::uniform_int_distribution<std::int64_t> small(-(std::int64_t{1} << 40), std::int64_t{1} << 40);
  const std::int64_t edges[] = {kMin, kMin + 1, -1, 0, 1, kMax - 1, kMax,
                                std::int64_t{1} << 32, -(std::int64_t{1} << 32)};
  for (const std::int64_t a : edges) {
    for (const std::int64_t b : edges) {
      ASSERT_EQ(qa::multiply(a, b), qa::multiply_portable(a, b)) << a << " * " << b;
    }
  }
  for (int i = 0; i < 200'000; ++i) {
    const std::int64_t a = i % 2 ? any(random) : small(random);
    const std::int64_t b = i % 3 ? any(random) : small(random);
    ASSERT_EQ(qa::multiply(a, b), qa::multiply_portable(a, b)) << a << " * " << b;
  }
}

TEST(Notional, ResultsCompareAsSigned128BitIntegers) {
  EXPECT_LT(qa::multiply(-1, 1), qa::multiply(0, 0));
  EXPECT_LT(qa::multiply(kMax, 1), qa::multiply(kMax, 2));
  EXPECT_LT(qa::multiply(kMin, kMax), qa::multiply(kMin, 1));
}

}  // namespace
