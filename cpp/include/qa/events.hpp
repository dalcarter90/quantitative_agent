#pragma once

// Messages that cross threads through the SPSC queues (ADR 0004).
//
// Each is fixed size, trivially copyable and standard layout, with every byte
// accounted for (explicit reserved bytes, no compiler padding), so a message
// can be copied, journaled and compared as raw bytes. Integers are in the
// machine's byte order (little-endian on every supported platform). The
// layouts are fixed by static_asserts in cpp/tests/test_events.cpp and
// documented in docs/architecture.md section 6.3; change all three together.
//
// MarketEvent, 88 bytes, one per book level or trade:
//
//   offset size field        meaning
//        0    8 recv_ts      wall-clock receive time (Timestamp)
//        8    8 recv_mono    monotonic receive stamp (MonoTimestamp); kHasMono
//       16    8 event_ts     venue time, or recv_ts if the venue gave none
//       24    8 seq          raw record number: replay order within a log
//       32    8 venue_seq    the venue's message number; kHasVenueSeq
//       40    8 trade_id     the venue's trade id, if numeric; kHasTradeId
//       48    8 price        Price mantissa
//       56    8 qty          Qty mantissa; 0 removes a book level
//       64    4 instrument   InstrumentId
//       68    4 checksum     the venue's book checksum after the message; kHasChecksum
//       72    4 level_index  position of this level in its message
//       76    2 depth        levels per side the venue keeps; kHasDepth
//       78    1 kind         EventKind
//       79    1 side         Side
//       80    1 flags        the k* bits below
//       81    7 reserved     zero

#include "qa/types.hpp"

#include <cstdint>

namespace qa {

// The book kinds use the same numbers as the Python schema's BookEventKind.
enum class EventKind : std::uint8_t {
  BookSnapshot = 0,
  BookDelta = 1,
  BookCheckSnapshot = 2,
  Trade = 3,
};

struct MarketEvent {
  static constexpr std::uint8_t kLastInMessage = 1u << 0;  // the message's last level
  static constexpr std::uint8_t kHasMono = 1u << 1;        // recv_mono is set
  static constexpr std::uint8_t kHasVenueSeq = 1u << 2;
  static constexpr std::uint8_t kHasChecksum = 1u << 3;
  static constexpr std::uint8_t kHasDepth = 1u << 4;
  static constexpr std::uint8_t kHasTradeId = 1u << 5;
  static constexpr std::uint8_t kEventTsFromReceipt = 1u << 6;  // ADR 0008

  Timestamp recv_ts;
  MonoTimestamp recv_mono;
  Timestamp event_ts;
  std::uint64_t seq = 0;
  std::int64_t venue_seq = 0;
  std::uint64_t trade_id = 0;
  Price price;
  Qty qty;
  InstrumentId instrument;
  std::uint32_t checksum = 0;
  std::uint32_t level_index = 0;
  std::uint16_t depth = 0;
  EventKind kind = EventKind::BookSnapshot;
  Side side = Side::Unknown;
  std::uint8_t flags = 0;
  std::uint8_t reserved[7] = {};

  [[nodiscard]] constexpr bool has(std::uint8_t flag) const noexcept {
    return (flags & flag) != 0;
  }
};

enum class RequestKind : std::uint8_t { New = 0, Cancel = 1 };
enum class OrderType : std::uint8_t { Limit = 0, Market = 1 };
enum class TimeInForce : std::uint8_t { GoodTillCancel = 0, ImmediateOrCancel = 1, PostOnly = 2 };

// OrderRequest, 64 bytes: from the core to an execution thread.
//
//   0 client_order_id  8 target_order_id (Cancel)  16 price  24 qty
//  32 created_ts  40 created_mono  48 instrument (4)  52 kind  53 side
//  54 type  55 time_in_force  56 reserved (8)
struct OrderRequest {
  std::uint64_t client_order_id = 0;
  std::uint64_t target_order_id = 0;
  Price price;
  Qty qty;
  Timestamp created_ts;
  MonoTimestamp created_mono;
  InstrumentId instrument;
  RequestKind kind = RequestKind::New;
  Side side = Side::Unknown;
  OrderType type = OrderType::Limit;
  TimeInForce time_in_force = TimeInForce::GoodTillCancel;
  std::uint8_t reserved[8] = {};
};

enum class ReportKind : std::uint8_t {
  Ack = 0,
  Reject = 1,
  PartialFill = 2,
  Fill = 3,
  Cancelled = 4,
};

// ExecutionReport, 80 bytes: from an execution thread back to the core.
//
//   0 client_order_id  8 exec_id  16 last_price  24 last_qty  32 leaves_qty
//  40 event_ts  48 recv_ts  56 recv_mono  64 instrument (4)  68 reject_code (2)
//  70 kind  71 side  72 reserved (8)
struct ExecutionReport {
  std::uint64_t client_order_id = 0;
  std::uint64_t exec_id = 0;
  Price last_price;
  Qty last_qty;
  Qty leaves_qty;
  Timestamp event_ts;
  Timestamp recv_ts;
  MonoTimestamp recv_mono;
  InstrumentId instrument;
  std::uint16_t reject_code = 0;
  ReportKind kind = ReportKind::Ack;
  Side side = Side::Unknown;
  std::uint8_t reserved[8] = {};
};

}  // namespace qa
