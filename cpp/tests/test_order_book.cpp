// The L2 order book (roadmap Phase 4b, ADR 0022): one MarketEvent at a time,
// the venue's own check after every message, stale and recovery by the
// reference book's rules, a fixed capacity, and instrument updates from the
// stream. Its agreement with the Python reference book on recorded data is
// tested through the bindings (python/tests/data/test_order_book.py).

#include "qa/order_book.hpp"

#include "book_events.hpp"
#include "qa/crc32.hpp"
#include "qa/events.hpp"
#include "qa/types.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using qa::BookCheck;
using qa::BookEffect;
using qa::BookLevel;
using qa::BookResult;
using qa::EventKind;
using qa::InstrumentId;
using qa::InstrumentMeta;
using qa::MarketEvent;
using qa::OrderBook;
using qa::Price;
using qa::Qty;
using qa::Side;
using qa::StaleReason;
using qa::StreamReason;
using qa::testkit::control;
using qa::testkit::frame;
using qa::testkit::Header;
using qa::testkit::instrument_update;
using qa::testkit::kInstrument;
using qa::testkit::kProbeSource;
using qa::testkit::L;
using qa::testkit::level;
using qa::testkit::snapshot;
using qa::testkit::update;

using Levels = std::vector<std::pair<std::int64_t, std::int64_t>>;
using Effects = std::vector<BookEffect>;

constexpr BookEffect kPending = BookEffect::Pending;
constexpr BookEffect kApplied = BookEffect::Applied;
constexpr BookEffect kRecovered = BookEffect::Recovered;
constexpr BookEffect kSkipped = BookEffect::Skipped;
constexpr BookEffect kWentStale = BookEffect::WentStale;
constexpr Side kBuy = Side::Buy;
constexpr Side kSell = Side::Sell;

// What a book reports is fixed size and plain; applying an event cannot throw.
static_assert(sizeof(BookResult) == 2 && std::is_trivially_copyable_v<BookResult>);
static_assert(noexcept(std::declval<OrderBook&>().apply(std::declval<const MarketEvent&>())));
static_assert(!std::is_copy_constructible_v<OrderBook> && !std::is_copy_assignable_v<OrderBook>);
// A StreamStale's reason is the book's reason of the same number (ADR 0022);
// the book's own reasons start at 64, clear of what StreamReason may gain.
static_assert(static_cast<int>(StaleReason::NotStale) == 0);
static_assert(static_cast<int>(StaleReason::SequenceGap) ==
              static_cast<int>(StreamReason::SequenceGap));
static_assert(static_cast<int>(StaleReason::SequenceRepeat) ==
              static_cast<int>(StreamReason::SequenceRepeat));
static_assert(static_cast<int>(StaleReason::SequenceOutOfOrder) ==
              static_cast<int>(StreamReason::SequenceOutOfOrder));
static_assert(static_cast<int>(StaleReason::ChecksumMismatch) ==
              static_cast<int>(StreamReason::ChecksumMismatch));
static_assert(static_cast<int>(StaleReason::MessageRejected) ==
              static_cast<int>(StreamReason::MessageRejected));
// The book's reason for each StreamReason is a switch with no default, held to
// reason_fits at build time, so a reason a StreamStale may gain fails the build
// until it is mapped (ADR 0023).
static_assert(qa::detail::every_stale_reason_is_mapped());
static_assert(qa::stale_reason_of(StreamReason::SequenceGap) == StaleReason::SequenceGap);
static_assert(qa::stale_reason_of(StreamReason::SequenceRepeat) == StaleReason::SequenceRepeat);
static_assert(qa::stale_reason_of(StreamReason::SequenceOutOfOrder) ==
              StaleReason::SequenceOutOfOrder);
static_assert(qa::stale_reason_of(StreamReason::ChecksumMismatch) ==
              StaleReason::ChecksumMismatch);
static_assert(qa::stale_reason_of(StreamReason::MessageRejected) == StaleReason::MessageRejected);
static_assert(qa::stale_reason_of(StreamReason::NoReason) == StaleReason::NotStale);
static_assert(qa::stale_reason_of(StreamReason::SessionStart) == StaleReason::NotStale);
static_assert(qa::stale_reason_of(StreamReason::Connect) == StaleReason::NotStale);
static_assert(qa::stale_reason_of(StreamReason::Disconnect) == StaleReason::NotStale);
static_assert(qa::stale_reason_of(static_cast<StreamReason>(200)) == StaleReason::NotStale);
static_assert(static_cast<int>(StaleReason::DeltaBeforeSnapshot) == 64);
static_assert(static_cast<int>(StaleReason::IncompleteMessage) == 65);
static_assert(static_cast<int>(StaleReason::CapacityExceeded) == 66);
static_assert(static_cast<int>(StaleReason::InvalidEvent) == 67);
static_assert(OrderBook::kDefaultCapacity == 65'536);
static_assert(OrderBook::capacity_for_depth(25) == 50);
static_assert(OrderBook::capacity_for_depth(1'000) == 2'000);
static_assert(OrderBook::capacity_for_depth(65'535) == 131'070);

// Mantissas that are the digits the venue prints: exponents and precisions 0.
constexpr InstrumentMeta kPlain{kInstrument, 0, 0, 0, 0, Price(1), Qty(1)};
// Kraken BTC-USD (ADR 0019): prices printed at one decimal, quantities at eight.
constexpr InstrumentMeta kKrakenBtc{kInstrument, 8, 8, 1, 8, Price(10'000'000), Qty(1)};

constexpr std::array kEveryReason = {
    StaleReason::SequenceGap,       StaleReason::SequenceRepeat,
    StaleReason::SequenceOutOfOrder, StaleReason::ChecksumMismatch,
    StaleReason::MessageRejected,   StaleReason::DeltaBeforeSnapshot,
    StaleReason::IncompleteMessage, StaleReason::CapacityExceeded,
    StaleReason::InvalidEvent};

OrderBook coinbase(std::uint32_t capacity = 64) {
  return OrderBook(kPlain, BookCheck::CoinbaseSequence, capacity);
}

OrderBook kraken(const InstrumentMeta& meta = kPlain, std::uint32_t capacity = 64) {
  return OrderBook(meta, BookCheck::KrakenChecksum, capacity);
}

Header numbered(std::int64_t venue_seq) {
  Header header;
  header.venue_seq = venue_seq;
  return header;
}

Header summed(std::uint32_t checksum, std::uint16_t depth = 10) {
  Header header;
  header.checksum = checksum;
  header.depth = depth;
  return header;
}

Effects feed(OrderBook& book, const std::vector<MarketEvent>& events) {
  Effects effects;
  for (const MarketEvent& event : events) {
    effects.push_back(book.apply(event).effect);
  }
  return effects;
}

BookEffect last(OrderBook& book, const std::vector<MarketEvent>& events) {
  return feed(book, events).back();
}

Levels side_of(const OrderBook& book, Side side) {
  std::vector<BookLevel> held(book.capacity());
  const std::uint32_t count = book.top(side, held);
  Levels out;
  for (std::uint32_t i = 0; i < count; ++i) {
    out.emplace_back(held[i].price.mantissa(), held[i].qty.mantissa());
  }
  return out;
}

std::uint64_t failures(const OrderBook& book) {
  std::uint64_t total = 0;
  for (const StaleReason reason : kEveryReason) {
    total += book.stale_count(reason);
  }
  return total;
}

// Kraken's recipe by hand: the digits of each level's price then quantity.
std::string digits(const Levels& asks, const Levels& bids) {
  std::string text;
  for (const auto& side : {asks, bids}) {
    for (const auto& [price, qty] : side) {
      text += std::to_string(price) + std::to_string(qty);
    }
  }
  return text;
}

const std::vector<L> kGood = {{kBuy, 100, 5}, {kBuy, 99, 1}, {kSell, 101, 3}};
const Levels kGoodBids = {{100, 5}, {99, 1}};
const Levels kGoodAsks = {{101, 3}};

// A Coinbase-rule book holding kGood from a snapshot at record 1.
OrderBook fresh() {
  OrderBook book = coinbase();
  EXPECT_EQ(last(book, snapshot(1, kGood, numbered(1))), kApplied);
  return book;
}

// --- snapshots and updates ----------------------------------------------------------------

TEST(OrderBook, StartsWithNoBookAndIsNotStale) {
  const OrderBook book = coinbase(50);
  EXPECT_FALSE(book.is_stale());
  EXPECT_FALSE(book.has_book());
  EXPECT_EQ(book.stale_reason(), StaleReason::NotStale);
  EXPECT_EQ(book.capacity(), 50u);
  EXPECT_EQ(book.check(), BookCheck::CoinbaseSequence);
  EXPECT_EQ(book.instrument().id, kInstrument);
  EXPECT_EQ(book.size(kBuy), 0u);
  EXPECT_EQ(book.best_bid(), std::nullopt);
  EXPECT_EQ(book.best_ask(), std::nullopt);
  EXPECT_EQ(failures(book), 0u);
  EXPECT_EQ(OrderBook(kPlain, BookCheck::KrakenChecksum).capacity(), 65'536u);
}

TEST(OrderBook, ASnapshotTakesEffectAtItsEnd) {
  OrderBook book = coinbase();
  const auto events = snapshot(1, kGood, numbered(1));
  for (std::size_t i = 0; i + 1 < events.size(); ++i) {
    EXPECT_EQ(book.apply(events[i]).effect, kPending) << i;
    EXPECT_FALSE(book.has_book()) << i;  // nothing can be read before the end
    EXPECT_EQ(book.size(kBuy), 0u) << i;
    EXPECT_EQ(book.best_bid(), std::nullopt) << i;
    EXPECT_EQ(side_of(book, kBuy), (Levels{})) << i;  // not the levels received so far
    EXPECT_EQ(side_of(book, kSell), (Levels{})) << i;
  }
  EXPECT_EQ(book.apply(events.back()), (BookResult{kApplied, StaleReason::NotStale}));
  EXPECT_TRUE(book.has_book());
  EXPECT_EQ(side_of(book, kBuy), kGoodBids);
  EXPECT_EQ(side_of(book, kSell), kGoodAsks);
}

TEST(OrderBook, AnUpdateTakesEffectAtItsLastLevelAndItsLevelsApplyInOrder) {
  OrderBook book = fresh();
  EXPECT_EQ(feed(book, update(2, {{kBuy, 98, 2}}, numbered(2))), (Effects{kApplied}));  // insert
  EXPECT_EQ(last(book, update(3, {{kSell, 102, 4}}, numbered(3))), kApplied);
  EXPECT_EQ(last(book, update(4, {{kBuy, 99, 7}}, numbered(4))), kApplied);   // change
  EXPECT_EQ(last(book, update(5, {{kSell, 101, 0}}, numbered(5))), kApplied);  // remove the best
  EXPECT_EQ(last(book, update(6, {{kBuy, 50, 0}}, numbered(6))), kApplied);    // not there
  // One price twice in a message: the last change wins.
  const auto twice = update(
      7, {{kBuy, 97, 1}, {kBuy, 97, 3}, {kSell, 104, 2}, {kSell, 104, 0}}, numbered(7));
  EXPECT_EQ(feed(book, twice), (Effects{kPending, kPending, kPending, kApplied}));
  EXPECT_EQ(side_of(book, kBuy), (Levels{{100, 5}, {99, 7}, {98, 2}, {97, 3}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{{102, 4}}));
  EXPECT_FALSE(book.is_stale());
}

// --- nothing can be read while a message is in progress (ADR 0023) -------------------------

void expect_nothing_shown(const OrderBook& book) {
  EXPECT_FALSE(book.has_book());
  EXPECT_EQ(side_of(book, kBuy), (Levels{}));
  EXPECT_EQ(side_of(book, kSell), (Levels{}));
  EXPECT_EQ(book.best_bid(), std::nullopt);
  EXPECT_EQ(book.best_ask(), std::nullopt);
  EXPECT_EQ(book.size(kBuy), 0u);
  EXPECT_EQ(book.size(kSell), 0u);
  EXPECT_EQ(book.quantity_at(kBuy, Price(100)), std::nullopt);
  EXPECT_EQ(book.checksum(), 0u);  // an empty book's
}

TEST(OrderBook, NothingCanBeReadWhileAnUpdateIsInProgress) {
  // Coinbase lists an update's bids before its asks. The market moves up a
  // tick: the bid at 101 arrives before the ask at 101 it crosses is removed,
  // so between the two levels the sides are crossed, a book the venue never had
  // (review 2026-11, finding 1).
  OrderBook book = coinbase();
  const std::vector<L> before = {{kBuy, 100, 5}, {kSell, 101, 3}, {kSell, 102, 4}};
  ASSERT_EQ(last(book, snapshot(1, before, numbered(1))), kApplied);
  const auto events = update(2, {{kBuy, 101, 2}, {kSell, 101, 0}}, numbered(2));
  ASSERT_EQ(book.apply(events[0]).effect, kPending);
  expect_nothing_shown(book);
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(book.apply(events[1]), (BookResult{kApplied, StaleReason::NotStale}));
  EXPECT_TRUE(book.has_book());
  EXPECT_EQ(side_of(book, kBuy), (Levels{{101, 2}, {100, 5}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{{102, 4}}));
}

TEST(OrderBook, NothingCanBeReadWhileAKrakenUpdateHoldsMoreThanItsDepth) {
  // Depth 2: an update that adds a bid holds three until the cut at its end.
  const std::uint32_t first = qa::crc32(digits({{102, 1}}, {{100, 1}, {99, 1}}));
  const std::uint32_t second = qa::crc32(digits({{102, 1}, {103, 1}}, {{101, 1}, {100, 1}}));
  OrderBook book = kraken(kPlain, OrderBook::capacity_for_depth(2));
  const std::vector<L> before = {{kBuy, 100, 1}, {kBuy, 99, 1}, {kSell, 102, 1}};
  ASSERT_EQ(last(book, snapshot(1, before, summed(first, 2))), kApplied);
  const auto events = update(2, {{kBuy, 101, 1}, {kSell, 103, 1}}, summed(second, 2));
  ASSERT_EQ(book.apply(events[0]).effect, kPending);
  expect_nothing_shown(book);
  EXPECT_EQ(book.apply(events[1]).effect, kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{101, 1}, {100, 1}}));
  EXPECT_EQ(book.checksum(), second);
}

TEST(OrderBook, AnUpdateWhoseLastLevelIsLostCannotBeReadUntilTheNextEventFailsIt) {
  OrderBook book = fresh();
  const auto events = update(2, {{kBuy, 98, 2}, {kSell, 102, 1}}, numbered(2));
  ASSERT_EQ(book.apply(events[0]).effect, kPending);
  expect_nothing_shown(book);  // however long nothing follows
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(book.apply(update(3, {{kBuy, 97, 1}}, numbered(3))[0]),
            (BookResult{kWentStale, StaleReason::IncompleteMessage}));
  expect_nothing_shown(book);
}

TEST(OrderBook, ABookThatCannotBeReadHasTheChecksumOfAnEmptyBook) {
  OrderBook book = kraken();
  EXPECT_EQ(book.checksum(), 0u);  // no snapshot yet
  const std::uint32_t good = qa::crc32(digits({{101, 3}}, {{100, 5}}));
  const auto events = snapshot(1, {{kBuy, 100, 5}, {kSell, 101, 3}}, summed(good));
  for (std::size_t i = 0; i + 1 < events.size(); ++i) {
    ASSERT_EQ(book.apply(events[i]).effect, kPending) << i;
    EXPECT_EQ(book.checksum(), 0u) << i;
  }
  ASSERT_EQ(book.apply(events.back()).effect, kApplied);
  EXPECT_EQ(book.checksum(), good);
  ASSERT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::ChecksumMismatch, 2)).effect,
            kWentStale);
  EXPECT_EQ(book.checksum(), 0u);
}

TEST(OrderBook, WhileANewSnapshotIsBuiltNeitherTheOldBookNorThePartOfTheNewOneCanBeRead) {
  OrderBook book = fresh();
  const auto events = snapshot(2, {{kBuy, 50, 1}, {kSell, 60, 2}}, numbered(0));
  for (std::size_t i = 0; i + 1 < events.size(); ++i) {
    ASSERT_EQ(book.apply(events[i]).effect, kPending) << i;
    if (i == 0) {  // a frame's begin says nothing yet: the old book stands
      EXPECT_TRUE(book.has_book());
      EXPECT_EQ(side_of(book, kBuy), kGoodBids);
      continue;
    }
    EXPECT_FALSE(book.has_book()) << i;
    EXPECT_EQ(side_of(book, kBuy), (Levels{})) << i;
    EXPECT_EQ(side_of(book, kSell), (Levels{})) << i;
    EXPECT_EQ(book.best_bid(), std::nullopt) << i;
    EXPECT_EQ(book.best_ask(), std::nullopt) << i;
    EXPECT_EQ(book.quantity_at(kBuy, Price(100)), std::nullopt) << i;
    EXPECT_EQ(book.size(kSell), 0u) << i;
  }
  EXPECT_EQ(book.apply(events.back()).effect, kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{50, 1}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{{60, 2}}));
}

TEST(OrderBook, ANewSnapshotReplacesTheWholeBook) {
  OrderBook book = fresh();
  EXPECT_EQ(last(book, snapshot(2, {{kBuy, 50, 1}, {kSell, 60, 2}}, numbered(0))), kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{50, 1}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{{60, 2}}));
}

TEST(OrderBook, ASnapshotsLevelsApplyInOrderLikeAnUpdates) {
  // A snapshot listing a price twice, and one with quantity 0: the reference
  // applies them in order, so the last wins and a zero removes.
  OrderBook book = coinbase();
  const std::vector<L> levels = {
      {kBuy, 100, 5}, {kBuy, 100, 6}, {kSell, 101, 3}, {kSell, 101, 0}, {kSell, 102, 0}};
  EXPECT_EQ(last(book, snapshot(1, levels, numbered(1))), kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{100, 6}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{}));
  EXPECT_TRUE(book.has_book());  // a book with an empty side is still a book
}

TEST(OrderBook, QueriesShowTheBestLevelsFirst) {
  OrderBook book = coinbase();
  const std::vector<L> levels = {{kBuy, 99, 7}, {kSell, 103, 9}, {kBuy, 100, 5}, {kSell, 101, 3}};
  ASSERT_EQ(last(book, snapshot(1, levels, numbered(1))), kApplied);
  EXPECT_EQ(book.best_bid(), (BookLevel{Price(100), Qty(5)}));
  EXPECT_EQ(book.best_ask(), (BookLevel{Price(101), Qty(3)}));
  EXPECT_EQ(book.size(kBuy), 2u);
  EXPECT_EQ(book.size(kSell), 2u);
  EXPECT_EQ(book.size(Side::Unknown), 0u);
  std::array<BookLevel, 1> one{};
  EXPECT_EQ(book.top(kSell, one), 1u);
  EXPECT_EQ(one[0], (BookLevel{Price(101), Qty(3)}));
  std::array<BookLevel, 5> five{};
  EXPECT_EQ(book.top(kBuy, five), 2u);  // only the levels there are
  EXPECT_EQ(five[1], (BookLevel{Price(99), Qty(7)}));
  EXPECT_EQ(book.top(Side::Unknown, five), 0u);
  EXPECT_EQ(book.quantity_at(kBuy, Price(99)), Qty(7));
  EXPECT_EQ(book.quantity_at(kBuy, Price(98)), std::nullopt);
  EXPECT_EQ(book.quantity_at(kSell, Price(99)), std::nullopt);  // a bid's price, asked of the asks
  EXPECT_EQ(book.quantity_at(Side::Unknown, Price(99)), std::nullopt);
}

// --- Kraken: depth and checksum -------------------------------------------------------------

TEST(OrderBook, KrakensChecksumFollowsTheDocumentedRecipe) {
  // Twelve levels a side; the checksum covers the ten best: asks ascending,
  // then bids descending, each as price digits then quantity digits.
  std::vector<L> levels;
  Levels asks;
  Levels bids;
  for (int i = 0; i < 12; ++i) {
    levels.push_back({kBuy, 1000 - i, 10 + i});
    levels.push_back({kSell, 1001 + i, 20 + i});
    if (i < 10) {
      bids.emplace_back(1000 - i, 10 + i);
      asks.emplace_back(1001 + i, 20 + i);
    }
  }
  const std::uint32_t expected = qa::crc32(digits(asks, bids));
  ASSERT_EQ(digits(asks, bids).substr(0, 12), "100120100221");
  OrderBook book = kraken();
  EXPECT_EQ(last(book, snapshot(1, levels, summed(expected, 12))), kApplied);
  EXPECT_EQ(book.checksum(), expected);
  EXPECT_EQ(book.size(kBuy), 12u);
}

TEST(OrderBook, KrakensChecksumIsOverTheDigitsTheVenuePrints) {
  // A real Kraken BTC/USD snapshot and its checksum (the adapters' test data):
  // prices printed at one decimal, quantities at eight, stored at exponent 8.
  const std::vector<L> levels = {{kBuy, 7'346'850'000'000, 20'416'909},
                                 {kBuy, 7'346'690'000'000, 340'289'101},
                                 {kSell, 7'346'860'000'000, 89'472'056},
                                 {kSell, 7'347'000'000'000, 208'204}};
  ASSERT_EQ(qa::crc32("73468689472056" "734700208204" "73468520416909" "734669340289101"),
            2'937'112'725u);
  OrderBook book = kraken(kKrakenBtc);
  EXPECT_EQ(last(book, snapshot(1, levels, summed(2'937'112'725u))), kApplied);
  EXPECT_EQ(book.checksum(), 2'937'112'725u);
  // At another precision the same book has other digits.
  InstrumentMeta two_decimals = kKrakenBtc;
  two_decimals.price_precision = 2;
  OrderBook other = kraken(two_decimals);
  EXPECT_EQ(last(other, snapshot(1, levels, summed(2'937'112'725u))), kWentStale);
  EXPECT_EQ(other.stale_reason(), StaleReason::ChecksumMismatch);
}

TEST(OrderBook, KrakensDigitsDivideLikeTheReferenceForZeroAndNegativePrices) {
  // Mantissas at exponent 2 printed at one decimal: divided by 10, rounding
  // down as Python's // does, with a minus sign where the result is negative.
  const InstrumentMeta meta{kInstrument, 2, 0, 1, 0, Price(1), Qty(1)};
  const std::vector<L> levels = {
      {kSell, -25, 7}, {kSell, 0, 1}, {kSell, 5, 2}, {kBuy, -30, 4}, {kBuy, -31, 5}};
  const std::uint32_t expected = qa::crc32("-37" "01" "02" "-34" "-45");
  OrderBook book = kraken(meta);
  EXPECT_EQ(last(book, snapshot(1, levels, summed(expected))), kApplied);
  EXPECT_EQ(book.checksum(), expected);
}

TEST(OrderBook, AKrakenBookIsCutToItsDepthWhenTheMessageEnds) {
  std::vector<L> levels;
  Levels ten;
  for (int i = 0; i < 12; ++i) {
    levels.push_back({kBuy, 100 - i, 1});
    if (i < 10) {
      ten.emplace_back(100 - i, 1);
    }
  }
  levels.push_back({kSell, 101, 1});
  OrderBook book = kraken();
  const std::uint32_t after_snapshot = qa::crc32(digits({{101, 1}}, ten));
  ASSERT_EQ(last(book, snapshot(1, levels, summed(after_snapshot))), kApplied);
  EXPECT_EQ(side_of(book, kBuy), ten);  // the two below the depth were cut

  // In one message: a level enters at the top and one leaves. Cut level by
  // level, the entering one would push 91 out before the removal made room.
  const Levels after = {{101, 2}, {99, 1}, {98, 1}, {97, 1}, {96, 1},
                        {95, 1},  {94, 1}, {93, 1}, {92, 1}, {91, 1}};
  const std::uint32_t after_update = qa::crc32(digits({{102, 1}}, after));
  const auto events =
      update(2, {{kBuy, 101, 2}, {kBuy, 100, 0}, {kSell, 101, 0}, {kSell, 102, 1}},
             summed(after_update));
  EXPECT_EQ(feed(book, events), (Effects{kPending, kPending, kPending, kApplied}));
  EXPECT_EQ(side_of(book, kBuy), after);
  EXPECT_EQ(side_of(book, kSell), (Levels{{102, 1}}));
}

TEST(OrderBook, AKrakenChecksumThatDiffersMakesTheBookStaleAndEmptiesIt) {
  const std::uint32_t good = qa::crc32(digits({{101, 3}}, {{100, 5}}));
  OrderBook book = kraken();
  ASSERT_EQ(last(book, snapshot(1, {{kBuy, 100, 5}, {kSell, 101, 3}}, summed(good))), kApplied);
  // The venue's book differs from ours after this update.
  const BookResult result = book.apply(update(2, {{kBuy, 100, 6}}, summed(good))[0]);
  EXPECT_EQ(result, (BookResult{kWentStale, StaleReason::ChecksumMismatch}));
  EXPECT_TRUE(book.is_stale());
  EXPECT_FALSE(book.has_book());
  EXPECT_EQ(book.stale_reason(), StaleReason::ChecksumMismatch);
  EXPECT_EQ(book.stale_count(StaleReason::ChecksumMismatch), 1u);
  EXPECT_EQ(side_of(book, kBuy), (Levels{}));
  EXPECT_EQ(book.best_ask(), std::nullopt);
}

TEST(OrderBook, AKrakenMessageWithoutAChecksumCannotBeVerified) {
  OrderBook book = kraken();
  Header no_checksum;
  no_checksum.depth = 10;
  EXPECT_EQ(last(book, snapshot(1, kGood, no_checksum)), kWentStale);
  EXPECT_EQ(book.stale_reason(), StaleReason::InvalidEvent);
  // A Coinbase header is not Kraken's either.
  OrderBook other = kraken();
  EXPECT_EQ(last(other, snapshot(1, kGood, numbered(1))), kWentStale);
  EXPECT_EQ(other.stale_reason(), StaleReason::InvalidEvent);
}

// --- Coinbase: message numbers ------------------------------------------------------------

TEST(OrderBook, CoinbasesNumbersMaySkipButMustIncrease) {
  OrderBook book = coinbase();
  ASSERT_EQ(last(book, snapshot(1, kGood, numbered(2))), kApplied);
  for (const std::int64_t number : {3, 7, 8, 20}) {  // other channels share the numbering
    EXPECT_EQ(last(book, update(2, {{kBuy, 98, number}}, numbered(number))), kApplied);
  }
  EXPECT_EQ(book.apply(update(3, {{kBuy, 97, 1}}, numbered(20))[0]),
            (BookResult{kWentStale, StaleReason::SequenceRepeat}));
  EXPECT_FALSE(book.has_book());

  OrderBook back = coinbase();
  ASSERT_EQ(last(back, snapshot(1, kGood, numbered(2))), kApplied);
  ASSERT_EQ(last(back, update(2, {{kBuy, 98, 1}}, numbered(5))), kApplied);
  EXPECT_EQ(back.apply(update(3, {{kBuy, 97, 1}}, numbered(4))[0]),
            (BookResult{kWentStale, StaleReason::SequenceOutOfOrder}));
}

TEST(OrderBook, ASnapshotRestartsCoinbasesNumbering) {
  OrderBook book = coinbase();
  ASSERT_EQ(last(book, snapshot(1, kGood, numbered(500))), kApplied);
  ASSERT_EQ(last(book, update(2, {{kBuy, 98, 1}}, numbered(501))), kApplied);
  EXPECT_EQ(last(book, snapshot(3, kGood, numbered(2))), kApplied);  // lower, and fine
  EXPECT_EQ(last(book, update(4, {{kBuy, 98, 1}}, numbered(3))), kApplied);
  EXPECT_EQ(last(book, update(5, {{kBuy, 98, 2}}, numbered(3))), kWentStale);
}

TEST(OrderBook, ACoinbaseMessageWithoutANumberCannotBeVerified) {
  OrderBook book = fresh();
  EXPECT_EQ(book.apply(update(2, {{kBuy, 98, 1}})[0]),
            (BookResult{kWentStale, StaleReason::InvalidEvent}));
  OrderBook other = fresh();  // Kraken's header is not Coinbase's
  EXPECT_EQ(other.apply(update(2, {{kBuy, 98, 1}}, summed(1))[0]),
            (BookResult{kWentStale, StaleReason::InvalidEvent}));
}

// --- stale, reset, recovery -------------------------------------------------------------------

TEST(OrderBook, AStreamStaleEventMakesTheBookStaleUntilASnapshotRecoversIt) {
  const std::pair<StreamReason, StaleReason> reasons[] = {
      {StreamReason::SequenceGap, StaleReason::SequenceGap},
      {StreamReason::SequenceRepeat, StaleReason::SequenceRepeat},
      {StreamReason::SequenceOutOfOrder, StaleReason::SequenceOutOfOrder},
      {StreamReason::ChecksumMismatch, StaleReason::ChecksumMismatch},
      {StreamReason::MessageRejected, StaleReason::MessageRejected}};
  for (const auto& [stream_reason, reason] : reasons) {
    OrderBook book = fresh();
    EXPECT_EQ(book.apply(control(EventKind::StreamStale, stream_reason, 2)),
              (BookResult{kWentStale, reason}));
    EXPECT_TRUE(book.is_stale());
    EXPECT_FALSE(book.has_book());
    EXPECT_EQ(book.stale_reason(), reason);
    EXPECT_EQ(book.stale_count(reason), 1u);
    EXPECT_EQ(failures(book), 1u);
    EXPECT_EQ(side_of(book, kBuy), (Levels{}));
    // Updates are not applied to a stale book, whatever they carry.
    const auto skipped = update(2, {{kBuy, 98, 1}, {kSell, 102, 1}}, numbered(2));
    EXPECT_EQ(feed(book, skipped), (Effects{kSkipped, kSkipped}));
    EXPECT_EQ(feed(book, update(3, {{kBuy, 97, 1}})), (Effects{kSkipped}));  // not even checked
    EXPECT_FALSE(book.has_book());
    // The next snapshot that passes its check recovers it.
    const auto recovery = feed(book, snapshot(4, kGood, numbered(9)));
    EXPECT_EQ(recovery.back(), kRecovered);
    EXPECT_FALSE(book.is_stale());
    EXPECT_EQ(book.stale_reason(), StaleReason::NotStale);
    EXPECT_EQ(side_of(book, kBuy), kGoodBids);
    EXPECT_EQ(last(book, update(5, {{kBuy, 98, 1}}, numbered(10))), kApplied);
    EXPECT_EQ(book.stale_count(reason), 1u);  // counted since the book was built
  }
}

TEST(OrderBook, AStaleEventWhileStaleIsCountedAndTheFirstReasonKept) {
  OrderBook book = fresh();
  ASSERT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::SequenceGap, 2)).effect,
            kWentStale);
  EXPECT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::SequenceOutOfOrder, 3)),
            (BookResult{kWentStale, StaleReason::SequenceOutOfOrder}));
  EXPECT_EQ(book.stale_reason(), StaleReason::SequenceGap);  // what began the stale period
  EXPECT_EQ(book.stale_count(StaleReason::SequenceGap), 1u);
  EXPECT_EQ(book.stale_count(StaleReason::SequenceOutOfOrder), 1u);
  EXPECT_EQ(book.stale_count(StaleReason::NotStale), 0u);
}

TEST(OrderBook, AResetEmptiesTheBookWithoutMakingItStale) {
  for (const StreamReason reason :
       {StreamReason::SessionStart, StreamReason::Connect, StreamReason::Disconnect}) {
    OrderBook book = fresh();
    EXPECT_EQ(book.apply(control(EventKind::StreamReset, reason, 2)),
              (BookResult{BookEffect::Reset, StaleReason::NotStale}));
    EXPECT_FALSE(book.is_stale());
    EXPECT_FALSE(book.has_book());
    EXPECT_EQ(side_of(book, kSell), (Levels{}));
    // The next message must be a snapshot; it is one, and no recovery.
    EXPECT_EQ(last(book, snapshot(3, kGood, numbered(0))), kApplied);
    EXPECT_EQ(failures(book), 0u);
  }
}

TEST(OrderBook, AnUpdateWithNoBookMakesItStale) {
  OrderBook never = coinbase();  // before any snapshot
  EXPECT_EQ(feed(never, update(1, {{kBuy, 100, 5}, {kSell, 101, 1}}, numbered(1))),
            (Effects{kWentStale, kSkipped}));
  EXPECT_EQ(never.stale_reason(), StaleReason::DeltaBeforeSnapshot);

  OrderBook book = fresh();  // after a reset
  ASSERT_EQ(book.apply(control(EventKind::StreamReset, StreamReason::Connect, 2)).effect,
            BookEffect::Reset);
  EXPECT_EQ(book.apply(update(2, {{kBuy, 98, 1}}, numbered(2))[0]),
            (BookResult{kWentStale, StaleReason::DeltaBeforeSnapshot}));
  EXPECT_TRUE(book.is_stale());
  EXPECT_EQ(book.stale_count(StaleReason::DeltaBeforeSnapshot), 1u);
}

TEST(OrderBook, AResetNeitherCausesNorEndsStalenessAndRestartsTheNumbering) {
  OrderBook book = fresh();
  ASSERT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::SequenceGap, 2)).effect,
            kWentStale);
  EXPECT_EQ(book.apply(control(EventKind::StreamReset, StreamReason::Disconnect, 3)).effect,
            BookEffect::Reset);
  EXPECT_EQ(book.apply(control(EventKind::StreamReset, StreamReason::Connect, 3)).effect,
            BookEffect::Reset);
  EXPECT_TRUE(book.is_stale());  // only a snapshot ends it
  EXPECT_EQ(last(book, update(4, {{kBuy, 98, 1}}, numbered(1))), kSkipped);
  EXPECT_EQ(last(book, snapshot(5, kGood, numbered(0))), kRecovered);

  // A new connection numbers its messages from the start again.
  OrderBook renumbered = coinbase();
  ASSERT_EQ(last(renumbered, snapshot(1, kGood, numbered(900))), kApplied);
  ASSERT_EQ(renumbered.apply(control(EventKind::StreamReset, StreamReason::Connect, 2)).effect,
            BookEffect::Reset);
  EXPECT_EQ(last(renumbered, snapshot(2, kGood, numbered(0))), kApplied);
  EXPECT_EQ(last(renumbered, update(3, {{kBuy, 98, 1}}, numbered(1))), kApplied);
}

TEST(OrderBook, ASnapshotThatFailsItsCheckDoesNotRecoverTheBook) {
  const std::uint32_t good = qa::crc32(digits({{101, 3}}, {{100, 5}}));
  const std::uint32_t other = qa::crc32(digits({{91, 1}}, {{90, 1}}));
  OrderBook book = kraken();
  ASSERT_EQ(last(book, snapshot(1, {{kBuy, 100, 5}, {kSell, 101, 3}}, summed(good))), kApplied);
  ASSERT_EQ(last(book, update(2, {{kBuy, 100, 6}}, summed(good))), kWentStale);
  // Wrong for itself: applied, checked, still stale.
  const std::vector<L> later = {{kBuy, 90, 1}, {kSell, 91, 1}};
  EXPECT_EQ(feed(book, snapshot(3, later, summed(good))),
            (Effects{kPending, kPending, kPending, kWentStale}));
  EXPECT_TRUE(book.is_stale());
  EXPECT_FALSE(book.has_book());
  EXPECT_EQ(last(book, update(4, {{kBuy, 89, 1}}, summed(good))), kSkipped);
  EXPECT_EQ(last(book, snapshot(5, later, summed(other))), kRecovered);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{90, 1}}));
  EXPECT_EQ(book.stale_count(StaleReason::ChecksumMismatch), 2u);
}

// --- frames -----------------------------------------------------------------------------------

TEST(OrderBook, ACheckSnapshotChangesNothingAndCannotRecoverAStaleBook) {
  OrderBook book = fresh();
  const std::vector<L> probe = {{kBuy, 1, 1}, {kSell, 2, 2}};
  const auto check = snapshot(2, probe, {}, EventKind::BookCheckSnapshot, kProbeSource);
  EXPECT_EQ(feed(book, check), (Effects{kPending, kPending, kPending, BookEffect::Ignored}));
  EXPECT_EQ(side_of(book, kBuy), kGoodBids);
  EXPECT_EQ(side_of(book, kSell), kGoodAsks);
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(last(book, update(3, {{kBuy, 98, 1}}, numbered(2))), kApplied);  // numbering unmoved

  // ADR 0020, decision 8: a check snapshot may not reseed a stale book.
  ASSERT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::MessageRejected, 4)).effect,
            kWentStale);
  const auto again = snapshot(5, probe, {}, EventKind::BookCheckSnapshot, kProbeSource);
  EXPECT_EQ(feed(book, again).back(), BookEffect::Ignored);
  EXPECT_TRUE(book.is_stale());
  EXPECT_FALSE(book.has_book());
  EXPECT_EQ(failures(book), 1u);
}

struct Damage {
  const char* name;
  std::vector<MarketEvent> events;
  bool recovers = false;  // the damage ends with a whole frame
};

std::vector<Damage> broken_frames() {
  // The frame: [0] begin, [1] bid 100, [2] bid 99, [3] ask 101 (last), [4] end.
  const auto f = snapshot(2, kGood, numbered(5));
  std::vector<Damage> out;
  out.push_back({"a level missing", {f[0], f[1], f[3], f[4]}});
  out.push_back({"the last level missing", {f[0], f[1], f[2], f[4]}});
  out.push_back({"no begin", {f[1], f[2], f[3], f[4]}});
  out.push_back({"levels out of position", {f[0], f[2], f[1], f[3], f[4]}});
  auto recounted = f;
  recounted[4].level_index = 2;
  out.push_back({"an end that counts differently", recounted});
  auto longer = f;
  longer[0].level_index = 4;
  longer[4].level_index = 4;
  out.push_back({"fewer levels than the frame says", longer});
  auto shorter = f;
  shorter[0].level_index = 2;
  shorter[4].level_index = 2;
  out.push_back({"more levels than the frame says", shorter});
  auto next = update(3, {{kBuy, 1, 1}}, numbered(9));
  out.push_back({"no end before the next message", {f[0], f[1], f[2], f[3], next[0]}});
  out.push_back({"another kind of event inside it",
                 {f[0], f[1], control(EventKind::StreamReset, StreamReason::Connect, 2), f[2],
                  f[3], f[4]}});
  out.push_back({"a level of another record inside it",
                 {f[0], f[1], level(EventKind::BookSnapshot, 9, 1, {kBuy, 5, 5}, false), f[2],
                  f[3], f[4]}});
  auto other_source = f;
  other_source[2].source = kProbeSource;
  out.push_back({"a level of another source inside it", other_source});
  auto check_level = f;
  check_level[2].kind = EventKind::BookCheckSnapshot;
  out.push_back({"a check level inside a stream snapshot", check_level});
  auto early_flag = f;
  early_flag[1].flags |= MarketEvent::kLastInMessage;
  out.push_back({"the last-level flag too early", early_flag});
  auto no_flag = f;
  no_flag[3].flags = static_cast<std::uint8_t>(no_flag[3].flags & ~MarketEvent::kLastInMessage);
  out.push_back({"no last-level flag", no_flag});
  auto end_elsewhere = f;
  end_elsewhere[4].seq = 3;
  out.push_back({"an end of another record", end_elsewhere});
  out.push_back({"an end with no frame", {f[4]}});
  out.push_back({"a begin inside it", {f[0], f[1], f[2], f[0], f[1], f[2], f[3], f[4]}, true});
  return out;
}

TEST(OrderBook, ASnapshotFrameThatIsNotCompleteMakesTheBookStale) {
  for (const Damage& damage : broken_frames()) {
    SCOPED_TRACE(damage.name);
    OrderBook book = fresh();
    const Effects effects = feed(book, damage.events);
    // One fault is one failure, however many of the frame's events follow it.
    EXPECT_EQ(std::count(effects.begin(), effects.end(), kWentStale), 1);
    EXPECT_EQ(book.stale_count(StaleReason::IncompleteMessage), 1u);
    EXPECT_EQ(failures(book), 1u);
    EXPECT_EQ(std::count(effects.begin(), effects.end(), kApplied), 0);
    if (damage.recovers) {
      EXPECT_EQ(effects.back(), kRecovered);
      EXPECT_FALSE(book.is_stale());
      continue;
    }
    EXPECT_EQ(std::count(effects.begin(), effects.end(), kRecovered), 0);
    EXPECT_TRUE(book.is_stale());
    EXPECT_FALSE(book.has_book());
    EXPECT_EQ(book.stale_reason(), StaleReason::IncompleteMessage);
    EXPECT_EQ(side_of(book, kBuy), (Levels{}));
    // A whole snapshot recovers it.
    EXPECT_EQ(last(book, snapshot(7, kGood, numbered(6))), kRecovered);
    EXPECT_EQ(side_of(book, kBuy), kGoodBids);
    EXPECT_EQ(side_of(book, kSell), kGoodAsks);
  }
}

TEST(OrderBook, ABrokenCheckSnapshotFrameMakesTheBookStaleToo) {
  // A frame cut short means events were lost; the book cannot know that only
  // the probe's were (ADR 0022).
  OrderBook book = fresh();
  const auto check =
      snapshot(2, {{kBuy, 1, 1}, {kSell, 2, 2}}, {}, EventKind::BookCheckSnapshot, kProbeSource);
  EXPECT_EQ(feed(book, {check[0], check[1], check[3]}), (Effects{kPending, kPending, kWentStale}));
  EXPECT_EQ(book.stale_reason(), StaleReason::IncompleteMessage);
  // A check level with no frame at all, likewise.
  OrderBook other = fresh();
  EXPECT_EQ(other.apply(check[1]), (BookResult{kWentStale, StaleReason::IncompleteMessage}));
  // And a stream snapshot's level inside a check snapshot's frame: the frame's
  // first level said whose it is.
  OrderBook mixed = fresh();
  auto inside = check;  // begin, two check levels (the second last), end
  inside[2].kind = EventKind::BookSnapshot;
  EXPECT_EQ(feed(mixed, inside), (Effects{kPending, kPending, kWentStale, kSkipped}));
  EXPECT_EQ(mixed.stale_reason(), StaleReason::IncompleteMessage);
  EXPECT_EQ(failures(mixed), 1u);
}

TEST(OrderBook, AnUpdateCutShortMakesTheBookStale) {
  const auto two = update(2, {{kBuy, 98, 2}, {kSell, 102, 1}}, numbered(2));
  {
    OrderBook book = fresh();  // another message's level arrives before the last
    ASSERT_EQ(book.apply(two[0]).effect, kPending);
    EXPECT_EQ(book.apply(update(3, {{kBuy, 97, 1}}, numbered(3))[0]),
              (BookResult{kWentStale, StaleReason::IncompleteMessage}));
    EXPECT_FALSE(book.has_book());
    EXPECT_EQ(failures(book), 1u);
  }
  {
    OrderBook book = fresh();  // a control event arrives before the last
    ASSERT_EQ(book.apply(two[0]).effect, kPending);
    // The event's own failure is what is reported; the cut-short message is counted.
    EXPECT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::SequenceGap, 3)),
              (BookResult{kWentStale, StaleReason::SequenceGap}));
    EXPECT_EQ(book.stale_reason(), StaleReason::IncompleteMessage);
    EXPECT_EQ(book.stale_count(StaleReason::IncompleteMessage), 1u);
    EXPECT_EQ(book.stale_count(StaleReason::SequenceGap), 1u);
  }
  {
    OrderBook book = fresh();  // a snapshot begins before the last: it recovers the book
    ASSERT_EQ(book.apply(two[0]).effect, kPending);
    const Effects effects = feed(book, snapshot(3, kGood, numbered(7)));
    EXPECT_EQ(effects.front(), kWentStale);
    EXPECT_EQ(effects.back(), kRecovered);
    EXPECT_EQ(side_of(book, kBuy), kGoodBids);  // nothing of the cut-short update remains
  }
  {
    OrderBook book = fresh();  // its first level lost
    EXPECT_EQ(book.apply(two[1]), (BookResult{kWentStale, StaleReason::IncompleteMessage}));
  }
  {
    OrderBook book = fresh();  // a level repeated
    ASSERT_EQ(book.apply(two[0]).effect, kPending);
    EXPECT_EQ(book.apply(two[0]), (BookResult{kWentStale, StaleReason::IncompleteMessage}));
  }
}

TEST(OrderBook, AnEmptySnapshotFrameMakesTheBookStale) {
  // Nothing in it says whether it is the stream's, and no header verifies it.
  OrderBook book = fresh();
  EXPECT_EQ(feed(book, snapshot(2, {})), (Effects{kPending, kWentStale}));
  EXPECT_TRUE(book.is_stale());
  EXPECT_EQ(book.stale_reason(), StaleReason::InvalidEvent);
  EXPECT_EQ(failures(book), 1u);
}

TEST(OrderBook, ALevelNoBookCanHoldMakesItStale) {
  const std::vector<L> bad[] = {{{Side::Unknown, 100, 1}}, {{kBuy, 100, -1}}};
  for (const auto& levels : bad) {
    OrderBook updated = fresh();
    EXPECT_EQ(updated.apply(update(2, levels, numbered(2))[0]),
              (BookResult{kWentStale, StaleReason::InvalidEvent}));
    EXPECT_FALSE(updated.has_book());
    // In a snapshot: the rest of the frame is skipped, and counted once.
    OrderBook rebuilt = fresh();
    std::vector<L> in_snapshot = {{kBuy, 90, 1}, levels[0], {kSell, 91, 1}};
    EXPECT_EQ(feed(rebuilt, snapshot(2, in_snapshot, numbered(2))),
              (Effects{kPending, kPending, kWentStale, kSkipped, kSkipped}));
    EXPECT_EQ(rebuilt.stale_reason(), StaleReason::InvalidEvent);
    EXPECT_EQ(failures(rebuilt), 1u);
  }
  // A zero or negative price is a price: the quality checks judge it, not the book.
  OrderBook book = fresh();
  EXPECT_EQ(last(book, update(2, {{kBuy, 0, 1}, {kBuy, -5, 1}}, numbered(2))), kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{100, 5}, {99, 1}, {0, 1}, {-5, 1}}));
}

// --- capacity (ADR 0022, decision 3) --------------------------------------------------------

TEST(OrderBook, ALevelBeyondTheCapacityMakesTheBookStaleUntilASnapshotFits) {
  OrderBook book = coinbase(3);
  const std::vector<L> full = {{kBuy, 100, 1}, {kBuy, 99, 1}, {kBuy, 98, 1}, {kSell, 101, 1}};
  ASSERT_EQ(last(book, snapshot(1, full, numbered(1))), kApplied);
  // One out, then one in: it fits. A held price needs no room.
  EXPECT_EQ(last(book, update(2, {{kBuy, 99, 0}, {kBuy, 97, 1}, {kBuy, 100, 9}}, numbered(2))),
            kApplied);
  EXPECT_EQ(book.size(kBuy), 3u);
  // A fourth bid: stale at that level, and the rest of the message is not applied.
  EXPECT_EQ(feed(book, update(3, {{kBuy, 96, 1}, {kSell, 102, 1}}, numbered(3))),
            (Effects{kWentStale, kSkipped}));
  EXPECT_TRUE(book.is_stale());
  EXPECT_FALSE(book.has_book());
  EXPECT_EQ(book.stale_reason(), StaleReason::CapacityExceeded);
  EXPECT_EQ(book.stale_count(StaleReason::CapacityExceeded), 1u);  // its own reason and a counter
  EXPECT_EQ(side_of(book, kBuy), (Levels{}));
  EXPECT_EQ(last(book, update(4, {{kSell, 103, 1}}, numbered(4))), kSkipped);
  // A snapshot that does not fit either leaves it stale, and is counted.
  const std::vector<L> four_asks = {
      {kSell, 101, 1}, {kSell, 102, 1}, {kSell, 103, 1}, {kSell, 104, 1}, {kBuy, 100, 1}};
  EXPECT_EQ(feed(book, snapshot(5, four_asks, numbered(5))),
            (Effects{kPending, kPending, kPending, kPending, kWentStale, kSkipped, kSkipped}));
  EXPECT_TRUE(book.is_stale());
  EXPECT_EQ(book.stale_count(StaleReason::CapacityExceeded), 2u);
  EXPECT_EQ(failures(book), 2u);
  // It recovers at the next snapshot that fits.
  const std::vector<L> fits = {{kBuy, 100, 1}, {kBuy, 99, 1}, {kBuy, 98, 1},
                               {kSell, 101, 1}, {kSell, 102, 1}, {kSell, 103, 1}};
  EXPECT_EQ(last(book, snapshot(6, fits, numbered(6))), kRecovered);
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(side_of(book, kBuy), (Levels{{100, 1}, {99, 1}, {98, 1}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{{101, 1}, {102, 1}, {103, 1}}));
  EXPECT_EQ(book.stale_count(StaleReason::CapacityExceeded), 2u);
}

TEST(OrderBook, ALevelThatDoesNotFitIsRefusedEvenIfALaterOneWouldMakeRoom) {
  OrderBook book = coinbase(3);
  const std::vector<L> full = {{kBuy, 100, 1}, {kBuy, 99, 1}, {kBuy, 98, 1}, {kSell, 101, 1}};
  ASSERT_EQ(last(book, snapshot(1, full, numbered(1))), kApplied);
  EXPECT_EQ(feed(book, update(2, {{kBuy, 97, 1}, {kBuy, 99, 0}}, numbered(2))),
            (Effects{kWentStale, kSkipped}));  // in before out
  EXPECT_EQ(book.stale_reason(), StaleReason::CapacityExceeded);
}

TEST(OrderBook, ACutBookNeedsRoomAboveItsDepthUntilTheMessageEnds) {
  // Depth 2, as Kraken would keep it: an update that adds a level holds three
  // until the cut. With a capacity of the depth that is one too many; with
  // capacity_for_depth it fits.
  const std::vector<L> two = {{kBuy, 100, 1}, {kBuy, 99, 1}, {kSell, 101, 1}};
  const std::uint32_t first = qa::crc32(digits({{101, 1}}, {{100, 1}, {99, 1}}));
  const std::uint32_t second = qa::crc32(digits({{101, 1}}, {{102, 1}, {100, 1}}));
  const auto adds = update(2, {{kBuy, 102, 1}}, summed(second, 2));

  OrderBook tight = kraken(kPlain, 2);
  ASSERT_EQ(last(tight, snapshot(1, two, summed(first, 2))), kApplied);
  EXPECT_EQ(tight.apply(adds[0]), (BookResult{kWentStale, StaleReason::CapacityExceeded}));

  OrderBook sized = kraken(kPlain, OrderBook::capacity_for_depth(2));
  ASSERT_EQ(sized.capacity(), 4u);
  ASSERT_EQ(last(sized, snapshot(1, two, summed(first, 2))), kApplied);
  EXPECT_EQ(sized.apply(adds[0]), (BookResult{kApplied, StaleReason::NotStale}));
  EXPECT_EQ(side_of(sized, kBuy), (Levels{{102, 1}, {100, 1}}));
}

TEST(OrderBook, AConfigurationItCannotBeBuiltWithIsRefused) {
  EXPECT_THROW(OrderBook(kPlain, BookCheck::CoinbaseSequence, 0), std::invalid_argument);
  EXPECT_THROW(OrderBook(kPlain, BookCheck::CoinbaseSequence, OrderBook::kMaxCapacity + 1),
               std::invalid_argument);
  EXPECT_THROW(OrderBook(kPlain, static_cast<BookCheck>(2), 8), std::invalid_argument);
  InstrumentMeta finer = kKrakenBtc;
  finer.price_precision = 9;  // above its exponent
  EXPECT_THROW(OrderBook(finer, BookCheck::KrakenChecksum, 8), std::invalid_argument);
  InstrumentMeta wide = kKrakenBtc;
  wide.qty_exp = 19;
  EXPECT_THROW(OrderBook(wide, BookCheck::KrakenChecksum, 8), std::invalid_argument);
  EXPECT_NO_THROW(OrderBook(kPlain, BookCheck::CoinbaseSequence, 1));
}

// --- instrument updates (ADR 0022, decision 2) ------------------------------------------------

TEST(OrderBook, AnInstrumentUpdateSwitchesTheChecksumDigitsWhereItArrives) {
  // Kraken moves BTC/USD from one decimal to two between two updates.
  const std::vector<L> levels = {{kBuy, 7'346'850'000'000, 20'416'909},
                                 {kSell, 7'346'860'000'000, 89'472'056}};
  const std::uint32_t one_decimal = qa::crc32("73468689472056" "73468520416909");
  const std::uint32_t one_decimal_after = qa::crc32("73468689472056" "73468530000000");
  const std::uint32_t two_decimals_after = qa::crc32("734686089472056" "734685030000000");
  const std::uint32_t two_decimals_later =
      qa::crc32("734685510000000" "734686089472056" "734685030000000");
  InstrumentMeta changed = kKrakenBtc;
  changed.price_precision = 2;
  changed.tick_size = Price(1'000'000);

  OrderBook book = kraken(kKrakenBtc);
  ASSERT_EQ(last(book, snapshot(1, levels, summed(one_decimal))), kApplied);
  EXPECT_EQ(book.apply(instrument_update(changed, 2)),
            (BookResult{BookEffect::Instrument, StaleReason::NotStale}));
  EXPECT_EQ(book.instrument().price_precision, 2);
  EXPECT_EQ(book.instrument().tick_size, Price(1'000'000));
  EXPECT_EQ(book.instrument().price_exp, 8);
  EXPECT_TRUE(book.has_book());  // the book itself is untouched: mantissas do not change
  EXPECT_EQ(book.checksum(), qa::crc32("734686089472056" "734685020416909"));
  // The updates after it are checked at two decimals.
  EXPECT_EQ(last(book, update(3, {{kBuy, 7'346'850'000'000, 30'000'000}},
                              summed(two_decimals_after))),
            kApplied);
  EXPECT_EQ(last(book, update(4, {{kSell, 7'346'855'000'000, 10'000'000}},
                              summed(two_decimals_later))),
            kApplied);
  EXPECT_EQ(failures(book), 0u);

  // Without the update the same message is a mismatch; and with it, a message
  // still summed at one decimal is.
  OrderBook unchanged = kraken(kKrakenBtc);
  ASSERT_EQ(last(unchanged, snapshot(1, levels, summed(one_decimal))), kApplied);
  EXPECT_EQ(unchanged.apply(update(3, {{kBuy, 7'346'850'000'000, 30'000'000}},
                                   summed(two_decimals_after))[0]),
            (BookResult{kWentStale, StaleReason::ChecksumMismatch}));
  OrderBook early = kraken(kKrakenBtc);
  ASSERT_EQ(last(early, snapshot(1, levels, summed(one_decimal))), kApplied);
  ASSERT_EQ(early.apply(instrument_update(changed, 2)).effect, BookEffect::Instrument);
  EXPECT_EQ(early.apply(update(3, {{kBuy, 7'346'850'000'000, 30'000'000}},
                               summed(one_decimal_after))[0]),
            (BookResult{kWentStale, StaleReason::ChecksumMismatch}));
}

TEST(OrderBook, AnInstrumentUpdateTheBookCannotFollowIsRefused) {
  const std::vector<L> levels = {{kBuy, 7'346'850'000'000, 20'416'909},
                                 {kSell, 7'346'860'000'000, 89'472'056}};
  const std::uint32_t sum = qa::crc32("73468689472056" "73468520416909");
  InstrumentMeta other_exponent = kKrakenBtc;
  other_exponent.price_exp = 7;  // every mantissa in the book would mean something else
  InstrumentMeta other_qty_exponent = kKrakenBtc;
  other_qty_exponent.qty_exp = 9;
  MarketEvent bad_scales = instrument_update(kKrakenBtc, 2);
  bad_scales.level_index = 8u | 8u << 8 | 9u << 16 | 8u << 24;  // price precision 9 > 8
  for (const MarketEvent& event : {instrument_update(other_exponent, 2),
                                   instrument_update(other_qty_exponent, 2), bad_scales}) {
    OrderBook book = kraken(kKrakenBtc);
    ASSERT_EQ(last(book, snapshot(1, levels, summed(sum))), kApplied);
    EXPECT_EQ(book.apply(event), (BookResult{kWentStale, StaleReason::InvalidEvent}));
    EXPECT_TRUE(book.is_stale());
    EXPECT_EQ(book.instrument().price_exp, 8);  // the metadata in force stays
    EXPECT_EQ(book.instrument().qty_exp, 8);
    EXPECT_EQ(book.instrument().price_precision, 1);
    // It recovers at a snapshot checked with the metadata it kept.
    EXPECT_EQ(last(book, snapshot(3, levels, summed(sum))), kRecovered);
  }
}

TEST(OrderBook, AnInstrumentUpdateIsTakenWhileStale) {
  // So the snapshot that recovers the book is checked at the precision in force.
  const std::vector<L> levels = {{kBuy, 7'346'850'000'000, 20'416'909},
                                 {kSell, 7'346'860'000'000, 89'472'056}};
  InstrumentMeta changed = kKrakenBtc;
  changed.price_precision = 2;
  OrderBook book = kraken(kKrakenBtc);
  ASSERT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::SequenceGap, 1)).effect,
            kWentStale);
  EXPECT_EQ(book.apply(instrument_update(changed, 2)).effect, BookEffect::Instrument);
  EXPECT_TRUE(book.is_stale());
  const std::uint32_t two_decimals = qa::crc32("734686089472056" "734685020416909");
  EXPECT_EQ(last(book, snapshot(3, levels, summed(two_decimals))), kRecovered);
}

TEST(OrderBook, ATickOrLotSizeBelowOneIsRefused) {
  // The store's schema requires both to be at least 1 (ADR 0023): a tick of 0
  // would divide by zero, a negative one invert a price collar.
  const std::pair<std::int64_t, std::int64_t> bad[] = {{0, 1}, {-1, 1}, {1, 0}, {1, -5}};
  for (const auto& [tick, lot] : bad) {
    SCOPED_TRACE(std::to_string(tick) + " " + std::to_string(lot));
    InstrumentMeta meta = kKrakenBtc;
    meta.tick_size = Price(tick);
    meta.lot_size = Qty(lot);
    EXPECT_TRUE(qa::scales_fit(meta));
    EXPECT_FALSE(qa::meta_fits(meta));
    EXPECT_THROW(OrderBook(meta, BookCheck::KrakenChecksum, 8), std::invalid_argument);
    // In the stream it is refused, and the metadata in force stays.
    OrderBook book = kraken(kKrakenBtc);
    EXPECT_EQ(book.apply(instrument_update(meta, 2)),
              (BookResult{kWentStale, StaleReason::InvalidEvent}));
    EXPECT_EQ(book.instrument().tick_size, Price(10'000'000));
    EXPECT_EQ(book.instrument().lot_size, Qty(1));
  }
  EXPECT_TRUE(qa::meta_fits(kKrakenBtc));
  EXPECT_TRUE(qa::meta_fits(kPlain));
}

// --- one stream per book (ADR 0023) -----------------------------------------------------------

constexpr std::uint32_t kOtherSource = 2;  // another log's stream

TEST(OrderBook, TheBookIsBoundToTheSourceOfTheStreamSnapshotThatBuildsIt) {
  OrderBook book = coinbase();
  EXPECT_EQ(book.bound_source(), std::nullopt);
  const auto events = snapshot(1, kGood, numbered(1), EventKind::BookSnapshot, kOtherSource);
  ASSERT_EQ(book.apply(events[0]).effect, kPending);
  EXPECT_EQ(book.bound_source(), std::nullopt);  // a frame's begin does not say whose it is
  ASSERT_EQ(book.apply(events[1]).effect, kPending);
  EXPECT_EQ(book.bound_source(), kOtherSource);  // its first level does
  EXPECT_EQ(last(book, {events.begin() + 2, events.end()}), kApplied);
  EXPECT_EQ(book.bound_source(), kOtherSource);
  // A check snapshot, which comes from the probe, binds nothing.
  const auto probe = snapshot(2, {{kBuy, 1, 1}}, {}, EventKind::BookCheckSnapshot, kProbeSource);
  EXPECT_EQ(last(book, probe), BookEffect::Ignored);
  EXPECT_EQ(book.bound_source(), kOtherSource);
}

TEST(OrderBook, AnUpdateOfAnotherSourceIsIgnoredAndCountedAndDoesNotMakeTheBookStale) {
  OrderBook book = fresh();  // bound to the stream source
  ASSERT_EQ(book.bound_source(), qa::testkit::kStreamSource);
  const auto other = update(2, {{kBuy, 98, 2}, {kSell, 102, 1}}, numbered(5), kOtherSource);
  EXPECT_EQ(feed(book, other), (Effects{BookEffect::Ignored, BookEffect::Ignored}));
  EXPECT_EQ(book.other_source_updates(), 1u);  // one update, counted once
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(failures(book), 0u);
  EXPECT_EQ(side_of(book, kBuy), kGoodBids);
  EXPECT_EQ(side_of(book, kSell), kGoodAsks);
  // Its number moved nothing: the stream's next is checked against the stream's last.
  EXPECT_EQ(last(book, update(3, {{kBuy, 98, 1}}, numbered(2))), kApplied);
  // Nor does it interrupt the stream's message in progress.
  const auto two = update(4, {{kBuy, 97, 1}, {kSell, 103, 1}}, numbered(3));
  ASSERT_EQ(book.apply(two[0]).effect, kPending);
  EXPECT_EQ(book.apply(other[0]).effect, BookEffect::Ignored);
  EXPECT_EQ(book.apply(other[1]).effect, BookEffect::Ignored);
  EXPECT_EQ(book.apply(two[1]).effect, kApplied);
  EXPECT_EQ(book.other_source_updates(), 2u);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{100, 5}, {99, 1}, {98, 1}, {97, 1}}));
  EXPECT_EQ(side_of(book, kSell), (Levels{{101, 3}, {103, 1}}));
}

TEST(OrderBook, ASnapshotOfAnotherSourceRebindsTheBook) {
  // Two logs of one instrument that overlap in time, merged by arrival
  // (review 2026-11, finding 2): log b's session start resets the book and b's
  // snapshot rebuilds it; then a late update of log a, numbered above b's
  // last, is not applied to b's book.
  OrderBook book = coinbase();
  ASSERT_EQ(last(book, snapshot(1, kGood, numbered(1))), kApplied);
  ASSERT_EQ(last(book, update(2, {{kBuy, 98, 1}}, numbered(2))), kApplied);
  MarketEvent b_starts = control(EventKind::StreamReset, StreamReason::SessionStart, 0);
  b_starts.source = kOtherSource;
  ASSERT_EQ(book.apply(b_starts).effect, BookEffect::Reset);
  const std::vector<L> b_levels = {{kBuy, 200, 1}, {kSell, 201, 1}};
  ASSERT_EQ(
      last(book, snapshot(1, b_levels, numbered(1), EventKind::BookSnapshot, kOtherSource)),
      kApplied);
  EXPECT_EQ(book.bound_source(), kOtherSource);
  EXPECT_EQ(book.apply(update(3, {{kBuy, 98, 7}}, numbered(3))[0]).effect, BookEffect::Ignored);
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(side_of(book, kBuy), (Levels{{200, 1}}));
  EXPECT_EQ(book.other_source_updates(), 1u);
  // b's own updates apply.
  EXPECT_EQ(last(book, update(2, {{kBuy, 199, 1}}, numbered(2), kOtherSource)), kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{200, 1}, {199, 1}}));

  // Without a reset in between, likewise.
  OrderBook direct = fresh();
  ASSERT_EQ(
      last(direct, snapshot(2, b_levels, numbered(1), EventKind::BookSnapshot, kOtherSource)),
      kApplied);
  EXPECT_EQ(direct.bound_source(), kOtherSource);
  EXPECT_EQ(direct.apply(update(3, {{kBuy, 98, 7}}, numbered(9))[0]).effect,
            BookEffect::Ignored);
  EXPECT_EQ(side_of(direct, kBuy), (Levels{{200, 1}}));
}

TEST(OrderBook, WhileAnotherSourcesSnapshotIsBuiltTheOldStreamsUpdatesAreIgnored) {
  OrderBook book = fresh();
  const auto b = snapshot(1, {{kBuy, 200, 1}, {kSell, 201, 1}}, numbered(1),
                          EventKind::BookSnapshot, kOtherSource);
  ASSERT_EQ(book.apply(b[0]).effect, kPending);
  ASSERT_EQ(book.apply(b[1]).effect, kPending);  // the book is b's from here
  EXPECT_EQ(book.apply(update(2, {{kBuy, 98, 1}}, numbered(2))[0]).effect, BookEffect::Ignored);
  EXPECT_EQ(last(book, {b[2], b[3]}), kApplied);
  EXPECT_EQ(side_of(book, kBuy), (Levels{{200, 1}}));
  EXPECT_EQ(failures(book), 0u);
}

TEST(OrderBook, AResetOrAFailureUnbindsTheBook) {
  // With nothing built, no update is of another stream: one from any source is
  // an update with no book, as the reference book has it.
  {
    OrderBook book = fresh();
    ASSERT_EQ(book.apply(control(EventKind::StreamReset, StreamReason::Connect, 2)).effect,
              BookEffect::Reset);
    EXPECT_EQ(book.bound_source(), std::nullopt);
    EXPECT_EQ(book.apply(update(3, {{kBuy, 98, 1}}, numbered(3), kOtherSource)[0]),
              (BookResult{kWentStale, StaleReason::DeltaBeforeSnapshot}));
    EXPECT_EQ(book.other_source_updates(), 0u);
  }
  {
    OrderBook book = fresh();
    ASSERT_EQ(book.apply(control(EventKind::StreamStale, StreamReason::SequenceGap, 2)).effect,
              kWentStale);
    EXPECT_EQ(book.bound_source(), std::nullopt);
    // Stale: no update applies, whatever its source.
    EXPECT_EQ(book.apply(update(3, {{kBuy, 98, 1}}, numbered(3), kOtherSource)[0]).effect,
              kSkipped);
    EXPECT_EQ(book.other_source_updates(), 0u);
    // Any stream's snapshot recovers it, and binds it.
    EXPECT_EQ(last(book, snapshot(4, kGood, numbered(9), EventKind::BookSnapshot, kOtherSource)),
              kRecovered);
    EXPECT_EQ(book.bound_source(), kOtherSource);
  }
}

// --- what is not the book's --------------------------------------------------------------------

TEST(OrderBook, EventsOfAnotherInstrumentAndTradesAreIgnored) {
  OrderBook book = fresh();
  MarketEvent other = update(2, {{kBuy, 98, 2}}, numbered(2))[0];
  other.instrument = InstrumentId(8u);
  EXPECT_EQ(book.apply(other), (BookResult{BookEffect::Ignored, StaleReason::NotStale}));
  MarketEvent stale = control(EventKind::StreamStale, StreamReason::SequenceGap, 2);
  stale.instrument = InstrumentId(8u);
  EXPECT_EQ(book.apply(stale).effect, BookEffect::Ignored);
  EXPECT_EQ(book.apply(level(EventKind::Trade, 3, 0, {kBuy, 100, 1}, false)).effect,
            BookEffect::Ignored);
  EXPECT_FALSE(book.is_stale());
  EXPECT_EQ(side_of(book, kBuy), kGoodBids);
  EXPECT_EQ(failures(book), 0u);
  // Another instrument's event does not interrupt this one's message either.
  const auto two = update(4, {{kBuy, 98, 2}, {kSell, 102, 1}}, numbered(2));
  ASSERT_EQ(book.apply(two[0]).effect, kPending);
  EXPECT_EQ(book.apply(other).effect, BookEffect::Ignored);
  EXPECT_EQ(book.apply(two[1]).effect, kApplied);
}

TEST(OrderBook, AnEventItCannotActOnMakesTheBookStale) {
  OrderBook unknown_kind = fresh();
  MarketEvent event = frame(EventKind::SnapshotBegin, 2, 0);
  event.kind = static_cast<EventKind>(9);
  EXPECT_EQ(unknown_kind.apply(event), (BookResult{kWentStale, StaleReason::InvalidEvent}));

  OrderBook no_reason = fresh();  // a StreamStale must say why
  EXPECT_EQ(no_reason.apply(control(EventKind::StreamStale, StreamReason::NoReason, 2)),
            (BookResult{kWentStale, StaleReason::InvalidEvent}));
  OrderBook reset_reason = fresh();
  EXPECT_EQ(reset_reason.apply(control(EventKind::StreamStale, StreamReason::Connect, 2)),
            (BookResult{kWentStale, StaleReason::InvalidEvent}));
}

}  // namespace
