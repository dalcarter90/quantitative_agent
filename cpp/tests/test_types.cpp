// Strong value types (ADR 0005): integer mantissas and nanosecond times that
// cannot be mixed with each other or turned into floating point implicitly.

#include "qa/types.hpp"

#include <gtest/gtest.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace {

using qa::InstrumentId;
using qa::InstrumentMeta;
using qa::MonoTimestamp;
using qa::Price;
using qa::Qty;
using qa::Side;
using qa::Timestamp;

template <class A, class B>
concept Addable = requires(A a, B b) { a + b; };
template <class A, class B>
concept Multipliable = requires(A a, B b) { a * b; };
template <class A, class B>
concept Comparable = requires(A a, B b) { a < b; };

// --- no implicit conversions, in either direction -----------------------------

template <class T>
constexpr bool kNoFloatingPoint =
    !std::is_convertible_v<T, double> && !std::is_convertible_v<T, float> &&
    !std::is_convertible_v<double, T> && !std::is_constructible_v<T, double> &&
    !std::is_constructible_v<T, float> && !std::is_constructible_v<double, T>;

static_assert(kNoFloatingPoint<Price>);
static_assert(kNoFloatingPoint<Qty>);
static_assert(kNoFloatingPoint<Timestamp>);
static_assert(kNoFloatingPoint<MonoTimestamp>);

// Integers only by explicit construction, and never back implicitly.
static_assert(std::is_constructible_v<Price, std::int64_t>);
static_assert(!std::is_convertible_v<std::int64_t, Price>);
static_assert(!std::is_convertible_v<Price, std::int64_t>);
static_assert(!std::is_convertible_v<Qty, std::int64_t>);
static_assert(!std::is_convertible_v<Timestamp, std::int64_t>);
static_assert(!std::is_constructible_v<Price, bool>);

// The types do not mix.
static_assert(!std::is_convertible_v<Price, Qty> && !std::is_constructible_v<Price, Qty>);
static_assert(!std::is_convertible_v<Qty, Price> && !std::is_constructible_v<Qty, Price>);
static_assert(!std::is_constructible_v<Timestamp, MonoTimestamp>);
static_assert(!std::is_constructible_v<MonoTimestamp, Timestamp>);
static_assert(!Addable<Price, Qty> && !Multipliable<Price, Qty> && !Comparable<Price, Qty>);
static_assert(!Multipliable<Price, Price> && !Multipliable<Price, std::int64_t>);
static_assert(!Comparable<Timestamp, MonoTimestamp>);
static_assert(Comparable<Price, Price> && Comparable<Qty, Qty> && Comparable<Timestamp, Timestamp>);

// Plain values that can cross threads (ADR 0004).
static_assert(std::is_trivially_copyable_v<Price> && sizeof(Price) == 8);
static_assert(std::is_trivially_copyable_v<Qty> && sizeof(Qty) == 8);
static_assert(std::is_trivially_copyable_v<Timestamp> && sizeof(Timestamp) == 8);
static_assert(std::is_trivially_copyable_v<MonoTimestamp> && sizeof(MonoTimestamp) == 8);
static_assert(std::is_trivially_copyable_v<InstrumentId> && sizeof(InstrumentId) == 4);

// An instrument id is built only from an integer that fits without loss
// (roadmap Phase 3; review, D7): before this, InstrumentId(int64_t{1} << 32)
// silently became id 0.
static_assert(std::is_constructible_v<InstrumentId, std::uint32_t>);
static_assert(std::is_constructible_v<InstrumentId, std::uint16_t>);
static_assert(std::is_constructible_v<InstrumentId, std::uint8_t>);
static_assert(!std::is_constructible_v<InstrumentId, std::uint64_t>);
static_assert(!std::is_constructible_v<InstrumentId, std::int64_t>);
static_assert(!std::is_constructible_v<InstrumentId, std::int32_t>);
static_assert(!std::is_constructible_v<InstrumentId, int>);
static_assert(!std::is_constructible_v<InstrumentId, bool>);
static_assert(!std::is_constructible_v<InstrumentId, double>);
static_assert(!std::is_convertible_v<std::uint32_t, InstrumentId>);
static_assert(std::is_trivially_copyable_v<InstrumentMeta>);
// ADR 0020: the venue's precisions (ADR 0019) fill what was padding, so the
// struct keeps its size and has no hidden bytes.
static_assert(sizeof(InstrumentMeta) == 24 && alignof(InstrumentMeta) == 8);
static_assert(offsetof(InstrumentMeta, id) == 0);
static_assert(offsetof(InstrumentMeta, price_exp) == 4);
static_assert(offsetof(InstrumentMeta, qty_exp) == 5);
static_assert(offsetof(InstrumentMeta, price_precision) == 6);
static_assert(offsetof(InstrumentMeta, qty_precision) == 7);
static_assert(offsetof(InstrumentMeta, tick_size) == 8);
static_assert(offsetof(InstrumentMeta, lot_size) == 16);
static_assert(std::has_unique_object_representations_v<InstrumentMeta>);

// Side uses the same numbers as the Python schema.
static_assert(static_cast<int>(Side::Sell) == -1);
static_assert(static_cast<int>(Side::Unknown) == 0);
static_assert(static_cast<int>(Side::Buy) == 1);
static_assert(sizeof(Side) == 1);

TEST(Types, MantissasRoundTripAtTheExtremes) {
  constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
  constexpr auto kMin = std::numeric_limits<std::int64_t>::min();
  for (const std::int64_t value : {kMin, std::int64_t{-1}, std::int64_t{0}, std::int64_t{7}, kMax}) {
    EXPECT_EQ(Price(value).mantissa(), value);
    EXPECT_EQ(Qty(value).mantissa(), value);
    EXPECT_EQ(Timestamp(value).ns(), value);
    EXPECT_EQ(MonoTimestamp(value).ns(), value);
  }
}

TEST(Types, ComparisonsOrderByMantissa) {
  EXPECT_LT(Price(734685), Price(734686));
  EXPECT_EQ(Qty(5), Qty(5));
  EXPECT_NE(Qty(5), Qty(6));
  EXPECT_GT(Timestamp(2), Timestamp(1));
  EXPECT_LE(MonoTimestamp(1), MonoTimestamp(1));
  EXPECT_EQ(Price{}, Price(0));
}

TEST(Types, InstrumentIdAndMetadata) {
  // Kraken BTC-USD: fixed exponents 8 and 8, printed at 1 and 8 decimals, a
  // tick of 0.1 and a lot of one satoshi (ADR 0019).
  constexpr InstrumentMeta kraken{InstrumentId(7u), 8, 8, 1, 8, Price(10'000'000), Qty(1)};
  EXPECT_EQ(kraken.id, InstrumentId(7u));
  EXPECT_EQ(kraken.id.value(), 7u);
  EXPECT_EQ(kraken.price_exp, 8);
  EXPECT_EQ(kraken.qty_exp, 8);
  EXPECT_EQ(kraken.price_precision, 1);
  EXPECT_EQ(kraken.qty_precision, 8);
  EXPECT_EQ(kraken.tick_size, Price(10'000'000));
  EXPECT_EQ(kraken.lot_size, Qty(1));
}

TEST(Types, ValidSidesForBooksAndTrades) {
  EXPECT_TRUE(qa::is_book_side(Side::Buy));
  EXPECT_TRUE(qa::is_book_side(Side::Sell));
  EXPECT_FALSE(qa::is_book_side(Side::Unknown));
  EXPECT_FALSE(qa::is_book_side(static_cast<Side>(3)));
}

}  // namespace
