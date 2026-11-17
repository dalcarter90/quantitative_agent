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
// MarketEvent, 88 bytes, one per book level, trade, or stream control event:
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
//       72    4 level_index  position of this level in its message; for SnapshotBegin
//                            and SnapshotEnd the number of levels; for StreamStale and
//                            StreamReset the stored stream event's event_index
//       76    2 depth        levels per side the venue keeps; kHasDepth
//       78    1 kind         EventKind
//       79    1 side         Side
//       80    1 flags        the k* bits below
//       81    1 reason       StreamReason: why a StreamStale or StreamReset (ADR 0020)
//       82    2 reserved     zero
//       84    4 source       the stream the event belongs to (ADR 0020)
//
// A source is one connection of one raw log: (log_id, "ws") or (log_id,
// "probe"). The harness (or a feed handler) numbers them and keeps the mapping
// outside the event. (source, seq) names the raw record; events of one record
// are contiguous, its control events before its levels.
//
// When a book message takes effect: an update's levels together, at the level
// flagged kLastInMessage; a snapshot's at its SnapshotEnd. A snapshot is
// SnapshotBegin, its levels (level_index 0 to n-1, the last flagged
// kLastInMessage), then SnapshotEnd, all with one source and seq and nothing
// else between them.

#include "qa/types.hpp"

#include <cstdint>

namespace qa {

// The book kinds use the same numbers as the Python schema's BookEventKind.
enum class EventKind : std::uint8_t {
  BookSnapshot = 0,       // a level of a snapshot of the stream's book
  BookDelta = 1,          // a level of an update; qty 0 removes the level
  BookCheckSnapshot = 2,  // a level of an independent snapshot (probe), not the stream
  Trade = 3,
  // Stream control and framing (ADR 0020). They carry no price, quantity, side,
  // trade id, checksum or depth.
  StreamStale = 4,    // the source's book cannot be trusted until its next snapshot
  StreamReset = 5,    // a new session or connection: the book is gone until a snapshot
  SnapshotBegin = 6,  // a snapshot's levels follow; level_index is how many
  SnapshotEnd = 7,    // they are complete; level_index is how many
};

// Why a StreamStale (1 to 5) or a StreamReset (6 to 8). Each is a stored stream
// event kind (docs/data-core-spec.md section 6); the Python reference book
// names them in snake case (book.STALE_REASONS, book.RESET_REASONS).
enum class StreamReason : std::uint8_t {
  NoReason = 0,
  SequenceGap = 1,
  SequenceRepeat = 2,
  SequenceOutOfOrder = 3,
  ChecksumMismatch = 4,
  MessageRejected = 5,
  SessionStart = 6,
  Connect = 7,
  Disconnect = 8,
};

[[nodiscard]] constexpr bool is_stream_control(EventKind kind) noexcept {
  return kind == EventKind::StreamStale || kind == EventKind::StreamReset ||
         kind == EventKind::SnapshotBegin || kind == EventKind::SnapshotEnd;
}

// A StreamStale says why with a stale reason, a StreamReset with a reset
// reason; every other kind has none.
[[nodiscard]] constexpr bool reason_fits(EventKind kind, StreamReason reason) noexcept {
  const auto value = static_cast<unsigned>(reason);
  if (kind == EventKind::StreamStale) {
    return value >= static_cast<unsigned>(StreamReason::SequenceGap) &&
           value <= static_cast<unsigned>(StreamReason::MessageRejected);
  }
  if (kind == EventKind::StreamReset) {
    return value >= static_cast<unsigned>(StreamReason::SessionStart) &&
           value <= static_cast<unsigned>(StreamReason::Disconnect);
  }
  return reason == StreamReason::NoReason;
}

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
  StreamReason reason = StreamReason::NoReason;
  std::uint8_t reserved[2] = {};
  std::uint32_t source = 0;

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
