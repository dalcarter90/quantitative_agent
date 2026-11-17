#pragma once

// MarketEvents for the order book's tests and benchmark: events built by hand
// as the Phase 4a converter makes them (ADR 0020, ADR 0021), and deterministic
// venue-like streams for the allocation test and the benchmark. Not part of
// qa_core: nothing here runs on the hot path, and the generators allocate.

#include "qa/crc32.hpp"
#include "qa/events.hpp"
#include "qa/types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace qa::testkit {

inline constexpr InstrumentId kInstrument{7u};
inline constexpr std::uint32_t kStreamSource = 0;
inline constexpr std::uint32_t kProbeSource = 1;

// A message's header, on every level of it: what its venue's check needs.
struct Header {
  std::optional<std::int64_t> venue_seq;
  std::optional<std::uint32_t> checksum;
  std::optional<std::uint16_t> depth;
};

struct L {
  Side side;
  std::int64_t price;
  std::int64_t qty;
};

[[nodiscard]] inline MarketEvent level(EventKind kind, std::uint64_t seq, std::uint32_t index,
                                       const L& l, bool last, const Header& header = {},
                                       std::uint32_t source = kStreamSource) {
  MarketEvent event{};
  event.kind = kind;
  event.seq = seq;
  event.source = source;
  event.instrument = kInstrument;
  event.level_index = index;
  event.side = l.side;
  event.price = Price(l.price);
  event.qty = Qty(l.qty);
  unsigned flags = last ? MarketEvent::kLastInMessage : 0u;
  if (header.venue_seq) {
    event.venue_seq = *header.venue_seq;
    flags |= MarketEvent::kHasVenueSeq;
  }
  if (header.checksum) {
    event.checksum = *header.checksum;
    flags |= MarketEvent::kHasChecksum;
  }
  if (header.depth) {
    event.depth = *header.depth;
    flags |= MarketEvent::kHasDepth;
  }
  event.flags = static_cast<std::uint8_t>(flags);
  return event;
}

// SnapshotBegin or SnapshotEnd of `count` levels.
[[nodiscard]] inline MarketEvent frame(EventKind kind, std::uint64_t seq, std::uint32_t count,
                                       std::uint32_t source = kStreamSource) {
  MarketEvent event{};
  event.kind = kind;
  event.seq = seq;
  event.source = source;
  event.instrument = kInstrument;
  event.level_index = count;
  event.flags = MarketEvent::kEventTsFromReceipt;
  return event;
}

// A StreamStale or StreamReset.
[[nodiscard]] inline MarketEvent control(EventKind kind, StreamReason reason, std::uint64_t seq) {
  MarketEvent event = frame(kind, seq, 0);
  event.reason = reason;
  return event;
}

[[nodiscard]] inline MarketEvent instrument_update(const InstrumentMeta& meta, std::uint64_t seq) {
  MarketEvent event = frame(EventKind::SnapshotBegin, seq, 0);
  set_instrument_meta(event, meta);
  return event;
}

// A framed snapshot: begin, its levels (the last flagged), end.
[[nodiscard]] inline std::vector<MarketEvent> snapshot(std::uint64_t seq,
                                                       const std::vector<L>& levels,
                                                       const Header& header = {},
                                                       EventKind kind = EventKind::BookSnapshot,
                                                       std::uint32_t source = kStreamSource) {
  const auto count = static_cast<std::uint32_t>(levels.size());
  std::vector<MarketEvent> events;
  events.reserve(levels.size() + 2);
  events.push_back(frame(EventKind::SnapshotBegin, seq, count, source));
  for (std::uint32_t i = 0; i < count; ++i) {
    events.push_back(level(kind, seq, i, levels[i], i + 1 == count, header, source));
  }
  events.push_back(frame(EventKind::SnapshotEnd, seq, count, source));
  return events;
}

// An update's levels, the last flagged.
[[nodiscard]] inline std::vector<MarketEvent> update(std::uint64_t seq,
                                                     const std::vector<L>& levels,
                                                     const Header& header = {}) {
  const auto count = static_cast<std::uint32_t>(levels.size());
  std::vector<MarketEvent> events;
  events.reserve(levels.size());
  for (std::uint32_t i = 0; i < count; ++i) {
    events.push_back(level(EventKind::BookDelta, seq, i, levels[i], i + 1 == count, header));
  }
  return events;
}

// --- venue-like streams ----------------------------------------------------------------------

// Kraken's checksum of a book held in maps, written from the venue's recipe
// with std::string, independently of the book's own: the ten best asks
// ascending then the ten best bids descending, each as the digits of its price
// then of its quantity (the mantissas here are the printed digits: precision
// equals exponent).
[[nodiscard]] inline std::uint32_t model_checksum(
    const std::map<std::int64_t, std::int64_t, std::greater<>>& bids,
    const std::map<std::int64_t, std::int64_t>& asks) {
  std::string digits;
  std::size_t taken = 0;
  for (auto it = asks.begin(); it != asks.end() && taken < 10; ++it, ++taken) {
    digits += std::to_string(it->first) + std::to_string(it->second);
  }
  taken = 0;
  for (auto it = bids.begin(); it != bids.end() && taken < 10; ++it, ++taken) {
    digits += std::to_string(it->first) + std::to_string(it->second);
  }
  return crc32(digits);
}

// One venue's stream for one instrument: a snapshot of `levels_per_side` levels
// a side, then `updates` update messages of one to three levels each, most of
// them near the top of the book. With a depth it is a Kraken-like stream: the
// book is cut to the depth after every message and every message carries the
// checksum of the result. Without one it is Coinbase-like: a full book, and a
// message number that increases. The same seed gives the same stream.
struct SyntheticStream {
  std::vector<MarketEvent> events;
  std::size_t snapshot_events = 0;  // how many of them are the snapshot's frame
  std::size_t update_messages = 0;
  std::uint64_t next_seq = 0;
  std::int64_t next_venue_seq = 0;
};

[[nodiscard]] inline SyntheticStream synthetic_stream(std::uint32_t levels_per_side,
                                                      std::size_t updates, std::uint64_t seed,
                                                      std::optional<std::uint16_t> depth = {}) {
  constexpr std::int64_t kMid = 7'346'850;
  std::mt19937_64 random(seed);
  std::map<std::int64_t, std::int64_t, std::greater<>> bids;
  std::map<std::int64_t, std::int64_t> asks;
  const auto quantity = [&random] { return static_cast<std::int64_t>(random() % 9'000 + 1); };
  const auto cut = [&] {
    if (!depth) {
      return;
    }
    while (bids.size() > *depth) {
      bids.erase(std::prev(bids.end()));
    }
    while (asks.size() > *depth) {
      asks.erase(std::prev(asks.end()));
    }
  };
  const auto header = [&](std::int64_t venue_seq) {
    Header h;
    if (depth) {
      h.depth = depth;
      h.checksum = model_checksum(bids, asks);
    } else {
      h.venue_seq = venue_seq;
    }
    return h;
  };

  SyntheticStream stream;
  std::vector<L> levels;
  for (std::uint32_t i = 0; i < levels_per_side; ++i) {  // best first, as the venues send it
    const std::int64_t price = kMid - 1 - static_cast<std::int64_t>(i);
    bids[price] = quantity();
    levels.push_back({Side::Buy, price, bids[price]});
  }
  for (std::uint32_t i = 0; i < levels_per_side; ++i) {
    const std::int64_t price = kMid + 1 + static_cast<std::int64_t>(i);
    asks[price] = quantity();
    levels.push_back({Side::Sell, price, asks[price]});
  }
  std::uint64_t seq = 1;
  std::int64_t venue_seq = 1;
  cut();
  stream.events = snapshot(seq, levels, header(venue_seq));
  stream.snapshot_events = stream.events.size();

  for (std::size_t n = 0; n < updates; ++n) {
    ++seq;
    venue_seq += static_cast<std::int64_t>(random() % 3 + 1);  // other channels share the numbers
    levels.clear();
    const std::size_t count = random() % 3 + 1;
    for (std::size_t i = 0; i < count; ++i) {
      const bool buy = random() % 2 == 0;
      // Nine in ten within eight ticks of the mid; the rest anywhere in the book.
      const std::uint64_t reach = random() % 10 == 0 ? levels_per_side + 4 : 8;
      const auto distance = static_cast<std::int64_t>(random() % reach + 1);
      const std::int64_t price = buy ? kMid - distance : kMid + distance;
      const std::int64_t qty = random() % 3 == 0 ? 0 : quantity();
      levels.push_back({buy ? Side::Buy : Side::Sell, price, qty});
      if (buy && qty == 0) {
        bids.erase(price);
      } else if (buy) {
        bids[price] = qty;
      } else if (qty == 0) {
        asks.erase(price);
      } else {
        asks[price] = qty;
      }
    }
    cut();
    const auto events = update(seq, levels, header(venue_seq));
    stream.events.insert(stream.events.end(), events.begin(), events.end());
  }
  stream.update_messages = updates;
  stream.next_seq = seq + 1;
  stream.next_venue_seq = venue_seq + 1;
  return stream;
}

}  // namespace qa::testkit
