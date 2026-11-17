// Messages that cross threads (ADR 0004): fixed size, trivially copyable, with
// the layout documented in qa/events.hpp and docs/architecture.md section 6.3.

#include "qa/events.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

namespace {

using qa::ExecutionReport;
using qa::MarketEvent;
using qa::OrderRequest;

static_assert(std::is_trivially_copyable_v<MarketEvent>);
static_assert(std::is_trivially_copyable_v<OrderRequest>);
static_assert(std::is_trivially_copyable_v<ExecutionReport>);
static_assert(std::is_standard_layout_v<MarketEvent>);
static_assert(std::is_standard_layout_v<OrderRequest>);
static_assert(std::is_standard_layout_v<ExecutionReport>);
static_assert(std::has_unique_object_representations_v<MarketEvent>);  // no hidden padding
static_assert(std::has_unique_object_representations_v<OrderRequest>);
static_assert(std::has_unique_object_representations_v<ExecutionReport>);

static_assert(sizeof(MarketEvent) == 88 && alignof(MarketEvent) == 8);
static_assert(offsetof(MarketEvent, recv_ts) == 0);
static_assert(offsetof(MarketEvent, recv_mono) == 8);
static_assert(offsetof(MarketEvent, event_ts) == 16);
static_assert(offsetof(MarketEvent, seq) == 24);
static_assert(offsetof(MarketEvent, venue_seq) == 32);
static_assert(offsetof(MarketEvent, trade_id) == 40);
static_assert(offsetof(MarketEvent, price) == 48);
static_assert(offsetof(MarketEvent, qty) == 56);
static_assert(offsetof(MarketEvent, instrument) == 64);
static_assert(offsetof(MarketEvent, checksum) == 68);
static_assert(offsetof(MarketEvent, level_index) == 72);
static_assert(offsetof(MarketEvent, depth) == 76);
static_assert(offsetof(MarketEvent, kind) == 78);
static_assert(offsetof(MarketEvent, side) == 79);
static_assert(offsetof(MarketEvent, flags) == 80);
// ADR 0020: two of the seven bytes that were reserved, so no offset moved.
static_assert(offsetof(MarketEvent, reason) == 81 && sizeof(MarketEvent::reason) == 1);
static_assert(offsetof(MarketEvent, reserved) == 82 && sizeof(MarketEvent::reserved) == 2);
static_assert(offsetof(MarketEvent, source) == 84 && sizeof(MarketEvent::source) == 4);

static_assert(sizeof(OrderRequest) == 64 && alignof(OrderRequest) == 8);
static_assert(offsetof(OrderRequest, client_order_id) == 0);
static_assert(offsetof(OrderRequest, target_order_id) == 8);
static_assert(offsetof(OrderRequest, price) == 16);
static_assert(offsetof(OrderRequest, qty) == 24);
static_assert(offsetof(OrderRequest, created_ts) == 32);
static_assert(offsetof(OrderRequest, created_mono) == 40);
static_assert(offsetof(OrderRequest, instrument) == 48);
static_assert(offsetof(OrderRequest, kind) == 52);
static_assert(offsetof(OrderRequest, side) == 53);
static_assert(offsetof(OrderRequest, type) == 54);
static_assert(offsetof(OrderRequest, time_in_force) == 55);

static_assert(sizeof(ExecutionReport) == 80 && alignof(ExecutionReport) == 8);
static_assert(offsetof(ExecutionReport, client_order_id) == 0);
static_assert(offsetof(ExecutionReport, exec_id) == 8);
static_assert(offsetof(ExecutionReport, last_price) == 16);
static_assert(offsetof(ExecutionReport, last_qty) == 24);
static_assert(offsetof(ExecutionReport, leaves_qty) == 32);
static_assert(offsetof(ExecutionReport, event_ts) == 40);
static_assert(offsetof(ExecutionReport, recv_ts) == 48);
static_assert(offsetof(ExecutionReport, recv_mono) == 56);
static_assert(offsetof(ExecutionReport, instrument) == 64);
static_assert(offsetof(ExecutionReport, reject_code) == 68);
static_assert(offsetof(ExecutionReport, kind) == 70);
static_assert(offsetof(ExecutionReport, side) == 71);

// Book event kinds use the same numbers as the Python schema's BookEventKind.
static_assert(static_cast<int>(qa::EventKind::BookSnapshot) == 0);
static_assert(static_cast<int>(qa::EventKind::BookDelta) == 1);
static_assert(static_cast<int>(qa::EventKind::BookCheckSnapshot) == 2);
static_assert(static_cast<int>(qa::EventKind::Trade) == 3);
// Stream control and snapshot framing (ADR 0020).
static_assert(static_cast<int>(qa::EventKind::StreamStale) == 4);
static_assert(static_cast<int>(qa::EventKind::StreamReset) == 5);
static_assert(static_cast<int>(qa::EventKind::SnapshotBegin) == 6);
static_assert(static_cast<int>(qa::EventKind::SnapshotEnd) == 7);
static_assert(static_cast<int>(qa::EventKind::InstrumentUpdate) == 8);  // ADR 0022
static_assert(MarketEvent::kInstrumentUpdateFlags ==
              (MarketEvent::kHasMono | MarketEvent::kEventTsFromReceipt));

// An InstrumentUpdate carries a whole InstrumentMeta (ADR 0022), and packing
// it is a constant expression.
constexpr qa::InstrumentMeta kKraken{qa::InstrumentId(7u), 8, 8, 1, 8, qa::Price(10'000'000),
                                     qa::Qty(1)};
constexpr MarketEvent kKrakenUpdate = [] {
  MarketEvent event{};
  qa::set_instrument_meta(event, kKraken);
  return event;
}();
static_assert(kKrakenUpdate.kind == qa::EventKind::InstrumentUpdate);
static_assert(kKrakenUpdate.level_index == (8u | 8u << 8 | 1u << 16 | 8u << 24));
static_assert(qa::instrument_meta(kKrakenUpdate).price_precision == 1);
static_assert(!qa::is_stream_control(qa::EventKind::InstrumentUpdate));  // it carries values
static_assert(qa::reason_fits(qa::EventKind::InstrumentUpdate, qa::StreamReason::NoReason));
static_assert(!qa::reason_fits(qa::EventKind::InstrumentUpdate, qa::StreamReason::Connect));
static_assert(sizeof(qa::StreamReason) == 1);
static_assert(static_cast<int>(qa::StreamReason::NoReason) == 0);
static_assert(static_cast<int>(qa::StreamReason::SequenceGap) == 1);
static_assert(static_cast<int>(qa::StreamReason::SequenceRepeat) == 2);
static_assert(static_cast<int>(qa::StreamReason::SequenceOutOfOrder) == 3);
static_assert(static_cast<int>(qa::StreamReason::ChecksumMismatch) == 4);
static_assert(static_cast<int>(qa::StreamReason::MessageRejected) == 5);
static_assert(static_cast<int>(qa::StreamReason::SessionStart) == 6);
static_assert(static_cast<int>(qa::StreamReason::Connect) == 7);
static_assert(static_cast<int>(qa::StreamReason::Disconnect) == 8);

TEST(Events, OnlyStreamControlKindsAreControl) {
  using qa::EventKind;
  EXPECT_FALSE(qa::is_stream_control(EventKind::BookSnapshot));
  EXPECT_FALSE(qa::is_stream_control(EventKind::BookDelta));
  EXPECT_FALSE(qa::is_stream_control(EventKind::BookCheckSnapshot));
  EXPECT_FALSE(qa::is_stream_control(EventKind::Trade));
  EXPECT_TRUE(qa::is_stream_control(EventKind::StreamStale));
  EXPECT_TRUE(qa::is_stream_control(EventKind::StreamReset));
  EXPECT_TRUE(qa::is_stream_control(EventKind::SnapshotBegin));
  EXPECT_TRUE(qa::is_stream_control(EventKind::SnapshotEnd));
}

TEST(Events, AReasonFitsOnlyItsKind) {
  using qa::EventKind;
  using qa::StreamReason;
  const StreamReason stale[] = {StreamReason::SequenceGap, StreamReason::SequenceRepeat,
                                StreamReason::SequenceOutOfOrder, StreamReason::ChecksumMismatch,
                                StreamReason::MessageRejected};
  const StreamReason reset[] = {StreamReason::SessionStart, StreamReason::Connect,
                                StreamReason::Disconnect};
  const EventKind others[] = {EventKind::BookSnapshot,  EventKind::BookDelta,
                              EventKind::BookCheckSnapshot, EventKind::Trade,
                              EventKind::SnapshotBegin, EventKind::SnapshotEnd};
  for (const StreamReason reason : stale) {
    EXPECT_TRUE(qa::reason_fits(EventKind::StreamStale, reason));
    EXPECT_FALSE(qa::reason_fits(EventKind::StreamReset, reason));
  }
  for (const StreamReason reason : reset) {
    EXPECT_TRUE(qa::reason_fits(EventKind::StreamReset, reason));
    EXPECT_FALSE(qa::reason_fits(EventKind::StreamStale, reason));
  }
  // A control event must say why; nothing else may.
  EXPECT_FALSE(qa::reason_fits(EventKind::StreamStale, StreamReason::NoReason));
  EXPECT_FALSE(qa::reason_fits(EventKind::StreamReset, StreamReason::NoReason));
  for (const EventKind kind : others) {
    EXPECT_TRUE(qa::reason_fits(kind, StreamReason::NoReason));
    EXPECT_FALSE(qa::reason_fits(kind, StreamReason::SequenceGap));
    EXPECT_FALSE(qa::reason_fits(kind, StreamReason::Connect));
  }
  EXPECT_FALSE(qa::reason_fits(EventKind::StreamStale, static_cast<StreamReason>(9)));
  EXPECT_FALSE(qa::reason_fits(EventKind::StreamReset, static_cast<StreamReason>(9)));
}

TEST(Events, AControlEventNamesItsSource) {
  MarketEvent event{};
  event.kind = qa::EventKind::StreamStale;
  event.reason = qa::StreamReason::SequenceGap;
  event.source = 0xFFFF'FFFFu;
  event.seq = 1790;
  MarketEvent copy;
  std::memcpy(&copy, &event, sizeof event);
  EXPECT_EQ(copy.reason, qa::StreamReason::SequenceGap);
  EXPECT_EQ(copy.source, 0xFFFF'FFFFu);
  const auto* bytes = reinterpret_cast<const unsigned char*>(&copy);
  EXPECT_EQ(bytes[81], 1u);
  EXPECT_EQ(bytes[82], 0u);
  EXPECT_EQ(bytes[83], 0u);
  EXPECT_EQ(bytes[84], 0xFFu);
}

TEST(Events, DefaultConstructedMessagesAreAllZeroBytes) {
  const MarketEvent event{};
  const OrderRequest request{};
  const ExecutionReport report{};
  const unsigned char zeros[88] = {};
  EXPECT_EQ(std::memcmp(&event, zeros, sizeof event), 0);
  EXPECT_EQ(std::memcmp(&request, zeros, sizeof request), 0);
  EXPECT_EQ(std::memcmp(&report, zeros, sizeof report), 0);
}

TEST(Events, AMarketEventCopiesByteForByte) {
  MarketEvent event{};
  event.recv_ts = qa::Timestamp(1'794'366'407'000'000'000);
  event.recv_mono = qa::MonoTimestamp(142'629'801'982'300);
  event.price = qa::Price(734685);
  event.qty = qa::Qty(20416909);
  event.kind = qa::EventKind::BookDelta;
  event.side = qa::Side::Buy;
  event.flags =
      static_cast<std::uint8_t>(qa::MarketEvent::kLastInMessage | qa::MarketEvent::kHasChecksum);
  event.checksum = 2694076524u;
  MarketEvent copy;
  std::memcpy(&copy, &event, sizeof event);
  EXPECT_EQ(std::memcmp(&copy, &event, sizeof event), 0);
  EXPECT_EQ(copy.price, qa::Price(734685));
  EXPECT_TRUE(copy.has(qa::MarketEvent::kHasChecksum));
  EXPECT_FALSE(copy.has(qa::MarketEvent::kHasVenueSeq));
}

TEST(Events, AnInstrumentUpdateCarriesEveryFieldOfInstrumentMeta) {
  const qa::InstrumentMeta metas[] = {
      kKraken,
      {qa::InstrumentId(0xFFFF'FFFFu), 18, 0, 18, 0, qa::Price(std::numeric_limits<std::int64_t>::max()),
       qa::Qty(std::numeric_limits<std::int64_t>::min())},
      {qa::InstrumentId(3u), 2, 5, 1, 3, qa::Price(-1), qa::Qty(0)}};
  for (const auto& meta : metas) {
    MarketEvent event{};
    event.seq = 41;
    event.source = 2;
    qa::set_instrument_meta(event, meta);
    MarketEvent copy;
    std::memcpy(&copy, &event, sizeof event);
    const qa::InstrumentMeta back = qa::instrument_meta(copy);
    EXPECT_EQ(std::memcmp(&back, &meta, sizeof meta), 0);
    EXPECT_EQ(copy.price, meta.tick_size);
    EXPECT_EQ(copy.qty, meta.lot_size);
    EXPECT_EQ(copy.instrument, meta.id);
    EXPECT_EQ((copy.seq), 41u);  // set_instrument_meta leaves the record's fields alone
    EXPECT_EQ(copy.source, 2u);
    EXPECT_TRUE(qa::scales_fit(back));
    const auto* bytes = reinterpret_cast<const unsigned char*>(&copy);
    EXPECT_EQ(bytes[72], static_cast<unsigned char>(meta.price_exp));
    EXPECT_EQ(bytes[75], static_cast<unsigned char>(meta.qty_precision));
    EXPECT_EQ(bytes[82], 0u);  // no reserved byte is used
    EXPECT_EQ(bytes[83], 0u);
  }
}

TEST(Events, ScalesFitAsTheStoresSchemaAllows) {
  EXPECT_TRUE(qa::scales_fit(kKraken));
  qa::InstrumentMeta meta = kKraken;
  meta.price_exp = 19;
  EXPECT_FALSE(qa::scales_fit(meta));
  meta = kKraken;
  meta.qty_exp = -1;
  EXPECT_FALSE(qa::scales_fit(meta));
  meta = kKraken;
  meta.price_precision = 9;  // above its exponent
  EXPECT_FALSE(qa::scales_fit(meta));
  meta = kKraken;
  meta.qty_precision = -1;
  EXPECT_FALSE(qa::scales_fit(meta));
  meta = kKraken;
  meta.price_exp = 0;
  meta.price_precision = 0;
  meta.qty_exp = 18;
  meta.qty_precision = 18;
  EXPECT_TRUE(qa::scales_fit(meta));
}

TEST(Events, FlagsAreDistinctBits) {
  const unsigned flags[] = {MarketEvent::kLastInMessage, MarketEvent::kHasMono,
                            MarketEvent::kHasVenueSeq,   MarketEvent::kHasChecksum,
                            MarketEvent::kHasDepth,      MarketEvent::kHasTradeId,
                            MarketEvent::kEventTsFromReceipt};
  unsigned seen = 0;
  for (const unsigned flag : flags) {
    EXPECT_EQ(flag & (flag - 1), 0u) << flag;  // one bit each
    EXPECT_EQ(seen & flag, 0u) << flag;
    seen |= flag;
  }
  // Review D7 (ADR 0020): kKnownFlags is exactly these, so the eighth bit is
  // unassigned and from_bytes refuses it.
  EXPECT_EQ(seen, MarketEvent::kKnownFlags);
  EXPECT_EQ(MarketEvent::kKnownFlags, 0x7Fu);
}

}  // namespace
