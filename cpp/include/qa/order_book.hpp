#pragma once

// The L2 order book of one instrument (roadmap Phase 4b, ADR 0022).
//
// It is given MarketEvents one at a time, as the Phase 4a converter and the
// Phase 6 feed handlers make them (ADR 0020, ADR 0021), and follows the Python
// reference book's rules (quantagent.data.book, resume mode):
//
// - An update's levels apply in order and the update takes effect at its
//   kLastInMessage level; a snapshot is built from its frame (SnapshotBegin,
//   levels, SnapshotEnd) and takes effect at its end. A check snapshot's
//   frame changes nothing. When a message takes effect the book is cut to the
//   message's depth, if it has one, and the venue's check runs: Kraken's
//   checksum, computed with the precisions in force, or Coinbase's message
//   numbers, which must increase.
// - The book goes stale on a StreamStale event, on its own check failing, on
//   an update with no book, on a message whose events did not all arrive in
//   order, on a level that does not fit its side's capacity, and on an event
//   it cannot act on. A stale book is emptied and takes no update; only a
//   complete stream snapshot that passes its check recovers it. A StreamReset
//   empties the book and neither causes nor ends staleness.
// - An InstrumentUpdate replaces the metadata in force, stale or not, if its
//   exponents are the book's.
//
// apply() never allocates, throws, locks or reads a clock: every level is held
// in storage allocated when the book is built. The constructor throws on a
// configuration it cannot build; it runs at startup.

#include "qa/book_side.hpp"
#include "qa/events.hpp"
#include "qa/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace qa {

// The integrity rule of the venue whose stream the book follows.
enum class BookCheck : std::uint8_t {
  KrakenChecksum = 0,    // a CRC32 of the ten best levels a side after every message
  CoinbaseSequence = 1,  // every message numbered, the numbers increasing
};

// Why the book went stale. 1 to 5 are the StreamReason of a StreamStale event
// with the same number, and also what the book's own check finds (4 for
// Kraken's checksum, 2 and 3 for Coinbase's numbers); the book's own reasons
// start at 64, clear of the reasons StreamReason will gain.
enum class StaleReason : std::uint8_t {
  NotStale = 0,
  SequenceGap = 1,
  SequenceRepeat = 2,
  SequenceOutOfOrder = 3,
  ChecksumMismatch = 4,
  MessageRejected = 5,
  DeltaBeforeSnapshot = 64,  // an update with no book: none since the start or a reset
  IncompleteMessage = 65,    // a message whose events did not all arrive, in order
  CapacityExceeded = 66,     // a level that did not fit its side's capacity
  InvalidEvent = 67,         // an event the book cannot act on
};

// What one event did.
enum class BookEffect : std::uint8_t {
  Pending = 0,     // part of a message that has not taken effect yet
  Applied = 1,     // a message took effect and the book is fresh
  Recovered = 2,   // a snapshot took effect and ended a stale period
  Skipped = 3,     // not applied: an update while stale, or the rest of a failed message
  WentStale = 4,   // the event made the book stale, or found a new failure while it was
  Reset = 5,       // a StreamReset emptied the book
  Instrument = 6,  // an InstrumentUpdate replaced the metadata in force
  Ignored = 7,     // nothing for this book: a check snapshot, a trade, another instrument
};

struct BookResult {
  BookEffect effect = BookEffect::Pending;
  StaleReason reason = StaleReason::NotStale;  // for WentStale: why

  friend constexpr bool operator==(BookResult, BookResult) noexcept = default;
};

class OrderBook {
 public:
  static constexpr std::uint32_t kDefaultCapacity = 65'536;
  static constexpr std::uint32_t kMaxCapacity = BookSide::kMaxCapacity;
  static constexpr std::uint32_t kChecksumLevels = 10;  // per side, in Kraken's checksum

  // The capacity per side of a book its venue keeps cut to `depth` levels: an
  // update's levels apply before the book is cut, so a side briefly holds
  // more than the depth (ADR 0022).
  [[nodiscard]] static constexpr std::uint32_t capacity_for_depth(std::uint32_t depth) noexcept {
    return 2 * depth;
  }

  // Throws std::invalid_argument for a capacity outside 1 to kMaxCapacity, a
  // check that is not a BookCheck, or scales that do not fit (scales_fit).
  OrderBook(const InstrumentMeta& instrument, BookCheck check,
            std::uint32_t capacity_per_side = kDefaultCapacity);

  OrderBook(OrderBook&&) noexcept = default;
  OrderBook& operator=(OrderBook&&) noexcept = default;
  OrderBook(const OrderBook&) = delete;
  OrderBook& operator=(const OrderBook&) = delete;
  ~OrderBook() = default;

  BookResult apply(const MarketEvent& event) noexcept;

  // Stale: the book cannot be trusted until a snapshot recovers it.
  [[nodiscard]] bool is_stale() const noexcept { return stale_; }
  // A snapshot has taken effect since the start, the last reset and the last
  // failure: the book can be read. The queries below show nothing otherwise.
  [[nodiscard]] bool has_book() const noexcept { return has_book_; }
  // What began the current stale period; NotStale when the book is not stale.
  [[nodiscard]] StaleReason stale_reason() const noexcept { return stale_reason_; }
  // How many failures of `reason` since the book was built.
  [[nodiscard]] std::uint64_t stale_count(StaleReason reason) const noexcept;

  // The best levels of one side, best first, into `out`; returns how many.
  [[nodiscard]] std::uint32_t top(Side side, std::span<BookLevel> out) const noexcept;
  [[nodiscard]] std::optional<BookLevel> best_bid() const noexcept;
  [[nodiscard]] std::optional<BookLevel> best_ask() const noexcept;
  [[nodiscard]] std::uint32_t size(Side side) const noexcept;
  [[nodiscard]] std::optional<Qty> quantity_at(Side side, Price price) const noexcept;
  // Kraken's checksum of the book as it is, at the precisions in force.
  [[nodiscard]] std::uint32_t checksum() const noexcept;

  [[nodiscard]] const InstrumentMeta& instrument() const noexcept { return meta_; }
  [[nodiscard]] BookCheck check() const noexcept { return check_; }
  [[nodiscard]] std::uint32_t capacity() const noexcept { return bids_.capacity(); }

 private:
  // Where the message being received stands.
  enum class Progress : std::uint8_t {
    Idle,
    FrameOpen,    // SnapshotBegin received, no level yet
    StreamFrame,  // a stream snapshot's levels
    CheckFrame,   // a check snapshot's levels
    FrameDone,    // every level received; SnapshotEnd next
    Update,       // an update's levels, before its last
  };

  static constexpr std::size_t kReasonSlots = 9;

  [[nodiscard]] BookResult begin(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult begin_update(const MarketEvent& event) noexcept;
  [[nodiscard]] bool continues(const MarketEvent& event) const noexcept;
  [[nodiscard]] bool next_level(const MarketEvent& event) const noexcept;
  [[nodiscard]] BookResult continue_message(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult update_level(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult stream_level(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult check_level(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult take_instrument(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult orphan() noexcept;
  [[nodiscard]] StaleReason apply_level(const MarketEvent& event) noexcept;
  [[nodiscard]] BookResult finish(bool snapshot) noexcept;
  [[nodiscard]] StaleReason verify() noexcept;
  BookResult fail(StaleReason reason) noexcept;
  void clear_book() noexcept;
  void take_header(const MarketEvent& event) noexcept;
  void start_message(Progress progress, const MarketEvent& event) noexcept;
  void set_divisors() noexcept;
  [[nodiscard]] const BookSide* side_of(Side side) const noexcept;

  InstrumentMeta meta_;
  BookCheck check_;
  BookSide bids_;
  BookSide asks_;
  std::int64_t price_divisor_ = 1;  // 10^(price_exp - price_precision): Kraken's digits
  std::int64_t qty_divisor_ = 1;

  bool stale_ = false;
  bool has_book_ = false;
  StaleReason stale_reason_ = StaleReason::NotStale;
  std::array<std::uint64_t, kReasonSlots> counts_{};
  bool has_last_venue_seq_ = false;
  std::int64_t last_venue_seq_ = 0;

  // The message in progress, and the header of its last level.
  Progress progress_ = Progress::Idle;
  bool frame_is_check_ = false;
  std::uint32_t message_source_ = 0;
  std::uint64_t message_seq_ = 0;
  std::uint32_t message_count_ = 0;  // a frame's level count
  std::uint32_t message_next_ = 0;   // the level_index the next level must have
  std::uint8_t header_flags_ = 0;
  std::int64_t header_venue_seq_ = 0;
  std::uint32_t header_checksum_ = 0;
  std::uint16_t header_depth_ = 0;
};

}  // namespace qa
