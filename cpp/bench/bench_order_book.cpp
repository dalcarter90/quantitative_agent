// The order book (roadmap Phase 4b): applying an update, reading the top of
// the book, Kraken's checksum, a Coinbase-sized snapshot, and the committed
// Coinbase fixture replayed message by message.
//
// Most streams are synthetic (cpp/tests/book_events.hpp), generated before
// timing: one to three levels per update, nine in ten within eight ticks of
// the mid and the rest anywhere in the book. The Kraken-like book keeps 25
// levels a side and checks a checksum after every message; the Coinbase-like
// one holds 19,266 levels a side (38,532 in all, as the Coinbase fixture's
// book) and checks message numbers. Real Coinbase updates are not like these:
// they have about five times the levels, most of them far from the top (review
// 2026-11, finding 3), so BM_ReplayCoinbaseFixture replays the recorded stream
// itself and reports each update's tail, not only the mean (ADR 0023).

#include "qa/order_book.hpp"

#include "book_events.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#if defined(_M_X64) || defined(__x86_64__)
#if defined(_MSC_VER)
#include <intrin.h>
#else
#include <x86intrin.h>
#endif
#define QA_BENCH_HAS_TSC 1
#endif

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

// Near the top only: every level of every update within eight ticks of the
// mid. A best case, not the typical one: on the recorded Coinbase stream 23%
// of update levels are within eight ranks of the best (review 2026-11).
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
  // Plain scales (exponents and precisions 0, so every divisor is 1) over the
  // synthetic stream's digits: 7-digit prices and quantities of at most 4
  // digits. Not Kraken BTC-USD's scales, whose prices are stored at exponent 8
  // and printed at one decimal (6 digits) and whose quantities have 8 decimals.
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

// --- the committed Coinbase fixture, converted (ADR 0023) --------------------------------------
//
// The recorded stream itself: the fixture's session start, metadata, snapshot
// (38,532 levels), its updates and its check snapshots, as the Phase 1c
// adapters and the Phase 4a converter make them. cpp/bench/export_events.py
// writes them, 88 bytes an event, to data/bench/coinbase-BTC-USD.events; the
// benchmark is skipped with an error until it has been run. Each iteration
// replays the whole stream on one book (its session start resets the book,
// so every pass is the venue's). Each update message is timed on its own with
// the CPU's timestamp counter (invariant on every x64 CPU this runs on;
// calibrated against the steady clock, whose resolution on Windows is 100 ns),
// so the counters give the tail per update: p50, p99, max and the mean. A
// reading costs a few ns, which is included.

constexpr const char* kFixtureEvents = QA_BENCH_DATA "/coinbase-BTC-USD.events";

struct FixtureStream {
  std::vector<qa::MarketEvent> events;
  InstrumentMeta meta{};
  std::string error;  // why the stream cannot be replayed, if it cannot
};

FixtureStream load_fixture() {
  FixtureStream stream;
  std::ifstream file(kFixtureEvents, std::ios::binary);
  if (!file) {
    stream.error = std::string("no ") + kFixtureEvents +
                   ": run .venv\\Scripts\\python cpp\\bench\\export_events.py first";
    return stream;
  }
  const std::vector<char> bytes((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
  if (bytes.empty() || bytes.size() % sizeof(qa::MarketEvent) != 0) {
    stream.error = std::string(kFixtureEvents) + " is not a whole number of MarketEvents";
    return stream;
  }
  stream.events.resize(bytes.size() / sizeof(qa::MarketEvent));
  std::memcpy(stream.events.data(), bytes.data(), bytes.size());
  const auto update = std::find_if(stream.events.begin(), stream.events.end(), [](const auto& e) {
    return e.kind == qa::EventKind::InstrumentUpdate;
  });
  if (update == stream.events.end()) {
    stream.error = std::string(kFixtureEvents) + " has no InstrumentUpdate to build the book from";
    return stream;
  }
  stream.meta = qa::instrument_meta(*update);
  return stream;
}

const FixtureStream& coinbase_fixture() {
  static const FixtureStream stream = load_fixture();
  return stream;
}

#if defined(QA_BENCH_HAS_TSC)
std::uint64_t ticks() noexcept { return __rdtsc(); }

// Nanoseconds per timestamp-counter tick, from 100 ms of the steady clock.
double ns_per_tick() {
  using Clock = std::chrono::steady_clock;
  const Clock::time_point start = Clock::now();
  const std::uint64_t first = ticks();
  while (Clock::now() - start < std::chrono::milliseconds(100)) {
  }
  const Clock::time_point stop = Clock::now();
  const std::uint64_t last = ticks();
  return std::chrono::duration<double, std::nano>(stop - start).count() /
         static_cast<double>(last - first);
}
#else
std::uint64_t ticks() noexcept {
  return static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

double ns_per_tick() {
  using Period = std::chrono::steady_clock::period;
  return 1e9 * static_cast<double>(Period::num) / static_cast<double>(Period::den);
}
#endif

double percentile(std::vector<std::uint64_t>& samples, double fraction) {
  const auto rank = static_cast<std::size_t>(fraction * static_cast<double>(samples.size() - 1));
  std::nth_element(samples.begin(), samples.begin() + static_cast<std::ptrdiff_t>(rank),
                   samples.end());
  return static_cast<double>(samples[rank]);
}

void BM_ReplayCoinbaseFixture(benchmark::State& state) {
  const FixtureStream& stream = coinbase_fixture();
  if (!stream.error.empty()) {
    state.SkipWithError(stream.error.c_str());
    return;
  }
  static const double tick_ns = ns_per_tick();
  const std::vector<qa::MarketEvent>& events = stream.events;
  OrderBook book(stream.meta, BookCheck::CoinbaseSequence);
  std::vector<std::uint64_t> pass;  // this pass's ticks per update message
  std::vector<std::uint64_t> updates;
  std::uint64_t snapshot_ticks = 0;
  std::int64_t snapshots = 0;
  std::int64_t update_levels = 0;
  bool failed = false;
  for (auto _ : state) {
    pass.clear();
    for (std::size_t i = 0; i < events.size() && !failed;) {
      const std::size_t first = i;
      const std::uint64_t start = ticks();
      qa::BookResult result;
      do {
        result = book.apply(events[i]);
        ++i;
      } while (result.effect == BookEffect::Pending && i < events.size());
      const std::uint64_t took = ticks() - start;
      const bool took_effect =
          result.effect == BookEffect::Applied || result.effect == BookEffect::Recovered;
      if (result.effect == BookEffect::WentStale || result.effect == BookEffect::Pending) {
        failed = true;  // the stream is not the venue's any more
      } else if (took_effect && events[first].kind == qa::EventKind::BookDelta) {
        pass.push_back(took);
        update_levels += static_cast<std::int64_t>(i - first);
      } else if (took_effect) {
        snapshot_ticks += took;
        ++snapshots;
      }
    }
    if (failed) {
      break;
    }
    state.PauseTiming();
    updates.insert(updates.end(), pass.begin(), pass.end());
    state.ResumeTiming();
  }
  if (failed) {
    state.SkipWithError("the book went stale, or the stream ended inside a message");
    return;
  }
  if (updates.empty()) {
    state.SkipWithError("the stream has no update messages");
    return;
  }
  const double count = static_cast<double>(updates.size());
  const double total = static_cast<double>(std::accumulate(updates.begin(), updates.end(),
                                                           std::uint64_t{0}));
  state.SetItemsProcessed(static_cast<std::int64_t>(updates.size()));
  state.counters["updates/pass"] = static_cast<double>(pass.size());
  state.counters["levels/update"] = static_cast<double>(update_levels) / count;
  state.counters["mean_ns"] = total / count * tick_ns;
  state.counters["p50_ns"] = percentile(updates, 0.50) * tick_ns;
  state.counters["p99_ns"] = percentile(updates, 0.99) * tick_ns;
  state.counters["max_ns"] = static_cast<double>(*std::max_element(updates.begin(), updates.end())) *
                             tick_ns;
  state.counters["snapshot_ms"] =
      static_cast<double>(snapshot_ticks) / static_cast<double>(std::max<std::int64_t>(snapshots, 1)) *
      tick_ns / 1e6;
}
BENCHMARK(BM_ReplayCoinbaseFixture)->Unit(benchmark::kMillisecond);

}  // namespace
