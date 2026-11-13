#pragma once

// Strong value types for prices, quantities and times (ADR 0005).
//
// Each wraps one integer. They are constructed explicitly from integers only,
// never convert implicitly to anything, do not mix with each other, and have
// no floating-point path in or out. Arithmetic between them (notional) goes
// through qa/notional.hpp.

#include <compare>
#include <concepts>
#include <cstdint>

namespace qa {

namespace detail {

// Integers that convert to int64 without loss, and are not bool.
template <class I>
concept LosslessToInt64 =
    std::integral<I> && !std::same_as<I, bool> && (std::signed_integral<I> || sizeof(I) < 8);

// One int64 value with a distinct type per Tag.
template <class Tag>
class Int64Value {
 public:
  constexpr Int64Value() noexcept = default;
  template <LosslessToInt64 I>
  constexpr explicit Int64Value(I value) noexcept : value_(static_cast<std::int64_t>(value)) {}
  // No floating point and no bool, even explicitly.
  template <std::floating_point F>
  explicit Int64Value(F) = delete;
  explicit Int64Value(bool) = delete;

  friend constexpr bool operator==(Int64Value, Int64Value) noexcept = default;
  friend constexpr auto operator<=>(Int64Value, Int64Value) noexcept = default;

 protected:
  std::int64_t value_ = 0;
};

}  // namespace detail

// A price as an integer mantissa: value = mantissa * 10^-price_exp.
class Price : public detail::Int64Value<Price> {
 public:
  using Int64Value::Int64Value;
  [[nodiscard]] constexpr std::int64_t mantissa() const noexcept { return value_; }
};

// A quantity as an integer mantissa: value = mantissa * 10^-qty_exp.
class Qty : public detail::Int64Value<Qty> {
 public:
  using Int64Value::Int64Value;
  [[nodiscard]] constexpr std::int64_t mantissa() const noexcept { return value_; }
};

// Wall-clock time: nanoseconds since the Unix epoch, UTC. A measurement: it can
// be stepped by a time service, so it is never an ordering key (ADR 0012).
class Timestamp : public detail::Int64Value<Timestamp> {
 public:
  using Int64Value::Int64Value;
  [[nodiscard]] constexpr std::int64_t ns() const noexcept { return value_; }
};

// The system's monotonic high-resolution counter, in nanoseconds (ADR 0012).
// Comparable only between readings on the same machine in the same boot.
class MonoTimestamp : public detail::Int64Value<MonoTimestamp> {
 public:
  using Int64Value::Int64Value;
  [[nodiscard]] constexpr std::int64_t ns() const noexcept { return value_; }
};

class InstrumentId {
 public:
  constexpr InstrumentId() noexcept = default;
  // Callers keep ids within 0..2^32-1; the Python bindings check it.
  template <std::integral I>
    requires(!std::same_as<I, bool>)
  constexpr explicit InstrumentId(I value) noexcept : value_(static_cast<std::uint32_t>(value)) {}
  template <std::floating_point F>
  explicit InstrumentId(F) = delete;
  explicit InstrumentId(bool) = delete;

  [[nodiscard]] constexpr std::uint32_t value() const noexcept { return value_; }
  friend constexpr bool operator==(InstrumentId, InstrumentId) noexcept = default;
  friend constexpr auto operator<=>(InstrumentId, InstrumentId) noexcept = default;

 private:
  std::uint32_t value_ = 0;
};

// The numbers match the Python schema's Side.
enum class Side : std::int8_t { Sell = -1, Unknown = 0, Buy = 1 };

// True for the two sides a book level can have.
[[nodiscard]] constexpr bool is_book_side(Side side) noexcept {
  return side == Side::Buy || side == Side::Sell;
}

// An instrument's representation, as in the store's `instruments` dataset.
struct InstrumentMeta {
  InstrumentId id;
  std::int8_t price_exp = 0;
  std::int8_t qty_exp = 0;
  Price tick_size;  // mantissa at price_exp
  Qty lot_size;     // mantissa at qty_exp
};

}  // namespace qa
