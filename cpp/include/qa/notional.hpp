#pragma once

// Notional value: price mantissa x quantity mantissa, exactly (ADR 0005).
//
// The product of two int64 mantissas can need up to 127 bits, so it is always
// computed in 128 bits: with the _mul128 intrinsic on MSVC x64, __int128 on
// GCC and Clang, and a portable fallback elsewhere. The result is at
// exponent price_exp + qty_exp. to_int64 says whether it fits back.

#include "qa/types.hpp"

#include <compare>
#include <cstdint>

#if defined(_MSC_VER) && defined(_M_X64)
#include <intrin.h>
#endif

namespace qa {

// A signed 128-bit integer: hi * 2^64 + lo, two's complement.
struct Int128 {
  std::int64_t hi = 0;
  std::uint64_t lo = 0;

  friend constexpr bool operator==(Int128, Int128) noexcept = default;
  friend constexpr std::strong_ordering operator<=>(Int128 a, Int128 b) noexcept {
    if (const auto order = a.hi <=> b.hi; order != 0) {
      return order;
    }
    return a.lo <=> b.lo;
  }
};

// a * b in 128 bits without any platform support. Used where no intrinsic
// exists, and tested against the intrinsic everywhere one does.
[[nodiscard]] constexpr Int128 multiply_portable(std::int64_t a, std::int64_t b) noexcept {
  const auto ua = static_cast<std::uint64_t>(a);
  const auto ub = static_cast<std::uint64_t>(b);
  const std::uint64_t a_lo = ua & 0xFFFFFFFFu;
  const std::uint64_t a_hi = ua >> 32;
  const std::uint64_t b_lo = ub & 0xFFFFFFFFu;
  const std::uint64_t b_hi = ub >> 32;

  const std::uint64_t p0 = a_lo * b_lo;
  const std::uint64_t p1 = a_lo * b_hi;
  const std::uint64_t p2 = a_hi * b_lo;
  const std::uint64_t p3 = a_hi * b_hi;
  const std::uint64_t middle = (p0 >> 32) + (p1 & 0xFFFFFFFFu) + (p2 & 0xFFFFFFFFu);
  const std::uint64_t lo = (p0 & 0xFFFFFFFFu) | (middle << 32);
  std::uint64_t hi = p3 + (p1 >> 32) + (p2 >> 32) + (middle >> 32);

  // That is the unsigned product. A negative operand x stands for x + 2^64, so
  // the unsigned product is too large by 2^64 times the other operand.
  if (a < 0) {
    hi -= ub;
  }
  if (b < 0) {
    hi -= ua;
  }
  return Int128{static_cast<std::int64_t>(hi), lo};
}

[[nodiscard]] inline Int128 multiply(std::int64_t a, std::int64_t b) noexcept {
#if defined(_MSC_VER) && defined(_M_X64)
  std::int64_t hi = 0;
  const std::int64_t lo = _mul128(a, b, &hi);
  return Int128{hi, static_cast<std::uint64_t>(lo)};
#elif defined(__SIZEOF_INT128__)
  const __int128 product = static_cast<__int128>(a) * b;
  return Int128{static_cast<std::int64_t>(product >> 64), static_cast<std::uint64_t>(product)};
#else
  return multiply_portable(a, b);
#endif
}

// The exact notional of a quantity at a price, at exponent price_exp + qty_exp.
[[nodiscard]] inline Int128 notional(Price price, Qty qty) noexcept {
  return multiply(price.mantissa(), qty.mantissa());
}

// Stores the value in `out` and returns true if it fits in int64.
[[nodiscard]] constexpr bool to_int64(Int128 value, std::int64_t& out) noexcept {
  const bool negative_lo = (value.lo >> 63) != 0;
  if (value.hi != (negative_lo ? -1 : 0)) {
    return false;
  }
  out = static_cast<std::int64_t>(value.lo);
  return true;
}

}  // namespace qa
