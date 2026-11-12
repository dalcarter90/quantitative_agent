# 0005: int64 mantissa with per-instrument exponent; int64 nanosecond timestamps

Status: Accepted (by the owner, as proposed), amended by 0008 and 0011
Date: 2026-11-10

## Context

Prices and quantities flow through the order book, risk checks, PnL and
storage, in both C++ and Python. Binary floating point cannot represent most
decimal prices exactly, which breaks price-level equality in a book and makes
replay comparisons fragile. Crypto spans a wide range: BTC near 10^5 with two
decimals, and tokens priced near 10^-8 traded in quantities above 10^12.

This choice is expensive to reverse once data is stored and code is written
against it.

## Decision

**Prices and quantities**

- `value = mantissa × 10^-exp`, with `mantissa` an `int64` and `exp` a small
  non-negative integer held per instrument (`price_exp`, `qty_exp`) in
  instrument metadata.
- `exp` is the finest decimal precision the venue supports for the instrument,
  not the current tick size. Tick size and lot size are separate validation
  rules.
- In C++, `Price` and `Qty` are distinct strong types wrapping `int64`, with no
  implicit conversion to each other or to floating point.
- Notional is computed through one helper using a 128-bit intermediate
  (`_mul128` on MSVC, `__int128` on GCC/Clang).
- Storage holds the integer mantissas. Instrument metadata, including
  exponents and tick sizes, is bitemporal.
- Python research code may convert to `float64` explicitly for features and
  models. Nothing converts floats back into order prices; order prices are
  built from integers.

**Timestamps**

- `int64` nanoseconds since the Unix epoch, UTC, for `event_ts`, `recv_ts` and
  `knowledge_ts`.
- A local `uint64` sequence number is assigned at receipt; replay order is
  `(recv_ts, seq)`.
- In-process latency is measured with a monotonic clock and stored as a
  duration.
- Hot-path components take time from an injected `Clock`, never from the
  system directly.

## Alternatives considered

- **`double`.** Simplest, and fine for signals. Not exact for prices, so
  book-level lookups and equality need epsilons, and replays can differ across
  compilers. Rejected for the book/order/risk/PnL path.
- **`int64` with one global scale (for example 10^-8 or 10^-9).** No
  per-instrument lookup and values are comparable across instruments. At
  10^-9 the largest quantity is about 9.2 × 10^9 units, which low-priced
  tokens exceed; at 10^-8 some venues' price precision is lost. Rejected on
  range.
- **Integer ticks (price = n × tick size).** Most compact and what many
  equity systems use. A tick-size change by the venue rescales every stored
  price for that instrument, which complicates point-in-time history.
- **Decimal types (`Decimal` in Python, a decimal128 library in C++).** Exact
  and self-describing, and a natural fit for a Python-only store. Too slow and
  too large for the C++ hot path; could still be the presentation type at the
  Python edge.
- **Timestamps as `datetime`/ISO strings or microseconds.** Microseconds lose
  precision some venues provide; strings and `datetime` are slow and ambiguous
  about time zone.
- **Monotonic time only.** Needed for latency, but cannot be compared with
  venue time or across restarts.

## Consequences

- Every price or quantity needs its instrument's exponent to be interpreted.
  APIs pass instrument ids alongside values, and cross-instrument arithmetic
  goes through helpers.
- An `int64` nanosecond timestamp covers years 1678 to 2262.
- If a venue increases an instrument's precision beyond its `exp`, the
  instrument gets a new metadata version with a larger `exp`; readers rescale
  using the metadata in force at each row's time.
- The Python data core is built to this representation from its first phase,
  so there is no conversion layer between the store and the C++ types.
