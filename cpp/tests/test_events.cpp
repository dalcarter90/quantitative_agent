// Messages that cross threads (ADR 0004): fixed size, trivially copyable, with
// the layout documented in qa/events.hpp and docs/architecture.md section 6.3.

#include "qa/events.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
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
}

}  // namespace
