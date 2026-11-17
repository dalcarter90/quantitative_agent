// The order book allocates nothing after it is built (roadmap Phase 4b;
// architecture section 5: no allocation on the hot path).
//
// This binary replaces the global operator new and delete with counting
// versions, so it is kept apart from qa_tests: only its own code (and qa_core,
// linked statically) goes through them. Phase 5 builds the reusable guard.
// Events are generated before each counted window; inside it there is nothing
// but the book, and no test macro.

#include "qa/order_book.hpp"

#include "book_events.hpp"
#include "qa/events.hpp"
#include "qa/types.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <vector>

#if defined(_MSC_VER)
#include <malloc.h>
#endif

namespace {

std::atomic<std::uint64_t> g_allocations{0};

void* allocate(std::size_t size) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  if (void* memory = std::malloc(size == 0 ? 1 : size)) {
    return memory;
  }
  throw std::bad_alloc();
}

void* allocate_aligned(std::size_t size, std::align_val_t alignment) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  const auto align = static_cast<std::size_t>(alignment);
#if defined(_MSC_VER)
  void* memory = _aligned_malloc(size == 0 ? 1 : size, align);
#else
  void* memory = std::aligned_alloc(align, ((size == 0 ? 1 : size) + align - 1) / align * align);
#endif
  if (memory == nullptr) {
    throw std::bad_alloc();
  }
  return memory;
}

void release_aligned(void* memory) noexcept {
#if defined(_MSC_VER)
  _aligned_free(memory);
#else
  std::free(memory);
#endif
}

}  // namespace

void* operator new(std::size_t size) { return allocate(size); }
void* operator new[](std::size_t size) { return allocate(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  try {
    return allocate(size);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  try {
    return allocate(size);
  } catch (...) {
    return nullptr;
  }
}
void* operator new(std::size_t size, std::align_val_t alignment) {
  return allocate_aligned(size, alignment);
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
  return allocate_aligned(size, alignment);
}
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete(void* memory, std::align_val_t) noexcept { release_aligned(memory); }
void operator delete[](void* memory, std::align_val_t) noexcept { release_aligned(memory); }
void operator delete(void* memory, std::size_t, std::align_val_t) noexcept {
  release_aligned(memory);
}
void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept {
  release_aligned(memory);
}

namespace {

using qa::BookCheck;
using qa::BookEffect;
using qa::BookLevel;
using qa::InstrumentMeta;
using qa::MarketEvent;
using qa::OrderBook;
using qa::Price;
using qa::Side;
using qa::StreamReason;
using qa::testkit::control;
using qa::testkit::instrument_update;
using qa::testkit::kInstrument;
using qa::testkit::synthetic_stream;
using qa::testkit::SyntheticStream;

constexpr InstrumentMeta kPlain{kInstrument, 0, 0, 0, 0, Price(1), qa::Qty(1)};

std::uint64_t allocations() { return g_allocations.load(std::memory_order_relaxed); }

// What the book did over a run, tallied inside the counted window.
struct Tally {
  std::array<std::uint64_t, 8> effects{};
  std::uint64_t events = 0;
  std::int64_t sink = 0;  // query results, so nothing is optimized away
};

// Every kind of thing a book does in steady state: the stream applied, a gap
// and the updates skipped while stale, a recovery, a reset, the stream again,
// an instrument update, a snapshot frame cut short, and a recovery from that.
std::vector<MarketEvent> steady_state(const SyntheticStream& stream) {
  const auto& all = stream.events;
  const std::vector<MarketEvent> updates(all.begin() + static_cast<std::ptrdiff_t>(stream.snapshot_events),
                                         all.end());
  std::vector<MarketEvent> run(all);
  run.push_back(control(qa::EventKind::StreamStale, StreamReason::SequenceGap, stream.next_seq));
  run.insert(run.end(), updates.begin(), updates.end());
  run.insert(run.end(), all.begin(), all.end());
  run.push_back(control(qa::EventKind::StreamReset, StreamReason::Connect, stream.next_seq + 1));
  run.insert(run.end(), all.begin(), all.end());
  run.push_back(instrument_update(kPlain, stream.next_seq + 2));
  // The snapshot again with one level lost: an incomplete frame.
  std::vector<MarketEvent> cut(all.begin(), all.begin() + static_cast<std::ptrdiff_t>(stream.snapshot_events));
  cut.erase(cut.begin() + 1);
  run.insert(run.end(), cut.begin(), cut.end());
  run.insert(run.end(), all.begin(), all.end());
  return run;
}

Tally drive(OrderBook& book, const std::vector<MarketEvent>& events) {
  Tally tally;
  std::array<BookLevel, 20> top{};
  for (const MarketEvent& event : events) {
    const auto result = book.apply(event);
    ++tally.effects[static_cast<std::size_t>(result.effect)];
    ++tally.events;
    if (result.effect == BookEffect::Applied || result.effect == BookEffect::Recovered) {
      const std::uint32_t bids = book.top(Side::Buy, top);
      tally.sink += bids == 0 ? 0 : top[0].price.mantissa();
      if (const auto ask = book.best_ask()) {
        tally.sink += ask->qty.mantissa();
      }
      tally.sink += book.checksum();
      tally.sink += book.quantity_at(Side::Sell, Price(7'346'851)).has_value() ? 1 : 0;
      tally.sink += static_cast<std::int64_t>(book.size(Side::Sell));
    }
  }
  return tally;
}

std::uint64_t effect(const Tally& tally, BookEffect which) {
  return tally.effects[static_cast<std::size_t>(which)];
}

TEST(AllocationCounter, CountsTheAllocationsOfThisBinary) {
  const std::uint64_t before = allocations();
  auto* number = new int(5);
  delete number;
  { const std::vector<int> values(10); }
  EXPECT_GE(allocations() - before, 2u);
}

TEST(OrderBookAllocations, ABookAllocatesItsStorageWhenItIsBuilt) {
  const std::uint64_t before = allocations();
  const OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  EXPECT_GT(allocations() - before, 0u);
  EXPECT_EQ(book.capacity(), OrderBook::kDefaultCapacity);
}

void expect_no_allocations(OrderBook& book, const SyntheticStream& stream) {
  const std::vector<MarketEvent> events = steady_state(stream);
  const std::uint64_t before = allocations();
  const Tally tally = drive(book, events);
  const std::uint64_t during = allocations() - before;

  EXPECT_EQ(during, 0u) << "operator new was called " << during << " times over "
                        << tally.events << " events";
  EXPECT_EQ(tally.events, events.size());
  // The run did what it was built to do: it was not a book stuck stale.
  EXPECT_EQ(effect(tally, BookEffect::WentStale), 2u);  // the gap and the cut-short frame
  EXPECT_EQ(effect(tally, BookEffect::Recovered), 2u);
  EXPECT_EQ(effect(tally, BookEffect::Reset), 1u);
  EXPECT_EQ(effect(tally, BookEffect::Instrument), 1u);
  // The stream four times over (one pass skipped while stale), two of its
  // snapshots applied and two recovering.
  EXPECT_EQ(effect(tally, BookEffect::Applied), 4 * stream.update_messages + 2);
  EXPECT_EQ(effect(tally, BookEffect::Skipped) >= stream.update_messages, true);
  EXPECT_FALSE(book.is_stale());
  EXPECT_NE(tally.sink, 0);
}

TEST(OrderBookAllocations, NoneInSteadyStateOnAKrakenLikeStream) {
  const SyntheticStream stream = synthetic_stream(25, 20'000, 11, std::uint16_t{25});
  OrderBook book(kPlain, BookCheck::KrakenChecksum, OrderBook::capacity_for_depth(25));
  expect_no_allocations(book, stream);
}

TEST(OrderBookAllocations, NoneInSteadyStateOnACoinbaseSizedBook) {
  // 38,532 levels, as the Coinbase fixture's book.
  const SyntheticStream stream = synthetic_stream(19'266, 20'000, 12);
  OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  expect_no_allocations(book, stream);
}

TEST(OrderBookAllocations, NoneWhenALevelDoesNotFit) {
  const SyntheticStream stream = synthetic_stream(40, 1'000, 13);
  const SyntheticStream fits = synthetic_stream(8, 1'000, 14);
  OrderBook book(kPlain, BookCheck::CoinbaseSequence, 16);
  std::vector<MarketEvent> events = stream.events;  // a snapshot of 40 a side: too many
  events.insert(events.end(), fits.events.begin(), fits.events.end());
  const std::uint64_t before = allocations();
  const Tally tally = drive(book, events);
  EXPECT_EQ(allocations() - before, 0u);
  EXPECT_EQ(book.stale_count(qa::StaleReason::CapacityExceeded), 1u);
  EXPECT_EQ(effect(tally, BookEffect::Recovered), 1u);
}

}  // namespace
