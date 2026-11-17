// The order book (roadmap Phase 4b): applying an update, reading the top of
// the book, Kraken's checksum, and a Coinbase-sized snapshot.
//
// Streams are synthetic (cpp/tests/book_events.hpp), generated before timing:
// one to three levels per update, nine in ten within eight ticks of the mid
// and the rest anywhere in the book. The Kraken-like book keeps 25 levels a
// side and checks a checksum after every message; the Coinbase-like one holds
// 19,266 levels a side (38,532 in all, as the Coinbase fixture's book) and
// checks message numbers.

#include "qa/order_book.hpp"

#include "book_events.hpp"

#include <benchmark/benchmark.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace {

using qa::BookCheck;
using qa::BookEffect;
using qa::BookLevel;
using qa::InstrumentMeta;
using qa::OrderBook;
using qa::Side;
using qa::testkit::synthetic_stream;
using qa::testkit::SyntheticStream;

constexpr InstrumentMeta kPlain{qa::testkit::kInstrument, 0, 0, 0, 0, qa::Price(1), qa::Qty(1)};
constexpr std::uint32_t kCoinbaseSide = 19'266;

void apply_snapshot(OrderBook& book, const SyntheticStream& stream) {
  for (std::size_t i = 0; i < stream.snapshot_events; ++i) {
    book.apply(stream.events[i]);
  }
}

// One update message per iteration, in stream order. At the end of the stream
// the snapshot is applied again, untimed, so every checksum and number stays
// the venue's.
void updates(benchmark::State& state, OrderBook& book, const SyntheticStream& stream) {
  apply_snapshot(book, stream);
  const auto& events = stream.events;
  std::size_t next = stream.snapshot_events;
  std::int64_t messages = 0;
  std::int64_t levels = 0;
  for (auto _ : state) {
    if (next == events.size()) {
      state.PauseTiming();
      apply_snapshot(book, stream);
      next = stream.snapshot_events;
      state.ResumeTiming();
    }
    qa::BookResult result;
    do {
      result = book.apply(events[next]);
      ++next;
      ++levels;
    } while (result.effect == BookEffect::Pending);
    benchmark::DoNotOptimize(result);
    ++messages;
  }
  if (book.is_stale()) {
    state.SkipWithError("the book went stale");
  }
  state.SetItemsProcessed(messages);
  state.counters["levels/msg"] =
      benchmark::Counter(static_cast<double>(levels) / static_cast<double>(messages));
}

void BM_ApplyUpdate_Kraken25(benchmark::State& state) {
  const SyntheticStream stream = synthetic_stream(25, 200'000, 1, std::uint16_t{25});
  OrderBook book(kPlain, BookCheck::KrakenChecksum, OrderBook::capacity_for_depth(25));
  updates(state, book, stream);
}
BENCHMARK(BM_ApplyUpdate_Kraken25);

void BM_ApplyUpdate_Coinbase38k(benchmark::State& state) {
  const SyntheticStream stream = synthetic_stream(kCoinbaseSide, 200'000, 2);
  OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  updates(state, book, stream);
}
BENCHMARK(BM_ApplyUpdate_Coinbase38k);

// Near the top only: what most updates of a liquid book are.
void BM_ApplyUpdate_Coinbase38k_Top(benchmark::State& state) {
  const SyntheticStream near = synthetic_stream(kCoinbaseSide, 200'000, 3);
  // Its snapshot, and only the updates all of whose levels are within eight
  // ticks of the mid, renumbered so the numbers still increase.
  SyntheticStream stream;
  std::size_t i = 0;
  for (; i < near.snapshot_events; ++i) {
    stream.events.push_back(near.events[i]);
  }
  constexpr std::int64_t kMid = 7'346'850;
  std::int64_t venue_seq = 1;
  std::vector<qa::MarketEvent> message;
  bool near_top = true;
  for (; i < near.events.size(); ++i) {
    const auto& event = near.events[i];
    message.push_back(event);
    const std::int64_t price = event.price.mantissa();
    near_top = near_top && price > kMid - 9 && price < kMid + 9;
    if (event.has(qa::MarketEvent::kLastInMessage)) {
      if (near_top) {
        ++venue_seq;
        for (auto& level : message) {
          level.venue_seq = venue_seq;
          stream.events.push_back(level);
        }
      }
      message.clear();
      near_top = true;
    }
  }
  stream.snapshot_events = near.snapshot_events;
  OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  updates(state, book, stream);
}
BENCHMARK(BM_ApplyUpdate_Coinbase38k_Top);

void BM_BestBidAndAsk_Coinbase38k(benchmark::State& state) {
  const SyntheticStream stream = synthetic_stream(kCoinbaseSide, 0, 4);
  OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  apply_snapshot(book, stream);
  for (auto _ : state) {
    std::optional<BookLevel> bid = book.best_bid();
    std::optional<BookLevel> ask = book.best_ask();
    benchmark::DoNotOptimize(bid);
    benchmark::DoNotOptimize(ask);
  }
}
BENCHMARK(BM_BestBidAndAsk_Coinbase38k);

void BM_Top20BothSides_Coinbase38k(benchmark::State& state) {
  const SyntheticStream stream = synthetic_stream(kCoinbaseSide, 0, 5);
  OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  apply_snapshot(book, stream);
  std::array<BookLevel, 20> bids{};
  std::array<BookLevel, 20> asks{};
  for (auto _ : state) {
    std::uint32_t found = book.top(Side::Buy, bids) + book.top(Side::Sell, asks);
    benchmark::DoNotOptimize(found);
    benchmark::ClobberMemory();
  }
}
BENCHMARK(BM_Top20BothSides_Coinbase38k);

void BM_KrakenChecksum(benchmark::State& state) {
  // Kraken BTC-USD's scales: prices at exponent 8 printed at 1, quantities at 8.
  const SyntheticStream stream = synthetic_stream(25, 0, 6, std::uint16_t{25});
  OrderBook book(kPlain, BookCheck::KrakenChecksum, OrderBook::capacity_for_depth(25));
  apply_snapshot(book, stream);
  for (auto _ : state) {
    std::uint32_t sum = book.checksum();
    benchmark::DoNotOptimize(sum);
  }
}
BENCHMARK(BM_KrakenChecksum);

void BM_ApplySnapshot_Coinbase38k(benchmark::State& state) {
  const SyntheticStream stream = synthetic_stream(kCoinbaseSide, 0, 7);
  OrderBook book(kPlain, BookCheck::CoinbaseSequence);
  for (auto _ : state) {
    apply_snapshot(book, stream);
    std::uint32_t bids = book.size(Side::Buy);
    benchmark::DoNotOptimize(bids);
  }
  if (!book.has_book()) {
    state.SkipWithError("the snapshot did not take effect");
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(stream.snapshot_events));
}
BENCHMARK(BM_ApplySnapshot_Coinbase38k)->Unit(benchmark::kMillisecond);

}  // namespace
