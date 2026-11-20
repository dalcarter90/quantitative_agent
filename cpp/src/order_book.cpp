#include "qa/order_book.hpp"

#include "qa/crc32.hpp"

#include <algorithm>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>

namespace qa {

namespace {

constexpr std::array<std::int64_t, kMaxExponent + 1> kPowersOfTen = [] {
  std::array<std::int64_t, kMaxExponent + 1> powers{};
  powers[0] = 1;
  for (std::size_t i = 1; i < powers.size(); ++i) {
    powers[i] = powers[i - 1] * 10;
  }
  return powers;
}();

// Python's //, which the reference book's checksum uses: rounds down.
constexpr std::int64_t floor_divide(std::int64_t value, std::int64_t divisor) noexcept {
  std::int64_t quotient = value / divisor;
  if (value % divisor != 0 && value < 0) {
    --quotient;
  }
  return quotient;
}

// The decimal digits of `value` as Python prints an int (a minus sign if
// negative), written at `out`; returns the end. At most 20 characters.
char* write_digits(char* out, std::int64_t value) noexcept {
  std::uint64_t magnitude = static_cast<std::uint64_t>(value);
  if (value < 0) {
    *out++ = '-';
    magnitude = std::uint64_t{0} - magnitude;
  }
  char reversed[20];
  int count = 0;
  do {
    reversed[count++] = static_cast<char>('0' + magnitude % 10);
    magnitude /= 10;
  } while (magnitude != 0);
  while (count > 0) {
    *out++ = reversed[--count];
  }
  return out;
}

constexpr std::size_t kNoSlot = ~std::size_t{0};

// Each StaleReason's counter (ADR 0023). As in stale_reason_of, every reason
// has a case and there is no default, so a reason StaleReason gains fails the
// build here until it has a counter.
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(error : 4062)  // an enumerator of the switched enum has no case
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"
#endif
constexpr std::size_t slot_of(StaleReason reason) noexcept {
  switch (reason) {
    case StaleReason::SequenceGap:
      return 0;
    case StaleReason::SequenceRepeat:
      return 1;
    case StaleReason::SequenceOutOfOrder:
      return 2;
    case StaleReason::ChecksumMismatch:
      return 3;
    case StaleReason::MessageRejected:
      return 4;
    case StaleReason::DeltaBeforeSnapshot:
      return 5;
    case StaleReason::IncompleteMessage:
      return 6;
    case StaleReason::CapacityExceeded:
      return 7;
    case StaleReason::InvalidEvent:
      return 8;
    case StaleReason::NotStale:
      return kNoSlot;
  }
  return kNoSlot;  // a byte that is no StaleReason
}
#if defined(_MSC_VER)
#pragma warning(pop)
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

// The counters are 0 to slots - 1, one reason each, and every reason a
// StreamStale may carry has one.
constexpr bool every_reason_has_a_counter(std::size_t slots) noexcept {
  std::size_t seen = 0;
  for (unsigned value = 0; value <= 0xFFu; ++value) {
    const std::size_t slot = slot_of(static_cast<StaleReason>(value));
    if (slot == kNoSlot) {
      continue;
    }
    if (slot >= slots) {
      return false;
    }
    for (unsigned other = 0; other < value; ++other) {
      if (slot_of(static_cast<StaleReason>(other)) == slot) {
        return false;
      }
    }
    ++seen;
  }
  for (unsigned value = 0; value <= 0xFFu; ++value) {
    const StaleReason mapped = stale_reason_of(static_cast<StreamReason>(value));
    if (mapped != StaleReason::NotStale && slot_of(mapped) == kNoSlot) {
      return false;
    }
  }
  return seen == slots;
}

const InstrumentMeta& checked(const InstrumentMeta& meta) {
  if (!meta_fits(meta)) {
    throw std::invalid_argument(
        "an order book's instrument needs exponents 0 to 18, precisions 0 to their "
        "exponents, and a tick and lot size of at least 1, not price " +
        std::to_string(meta.price_exp) + "/" + std::to_string(meta.price_precision) +
        " and quantity " + std::to_string(meta.qty_exp) + "/" +
        std::to_string(meta.qty_precision) + ", tick " +
        std::to_string(meta.tick_size.mantissa()) + " and lot " +
        std::to_string(meta.lot_size.mantissa()));
  }
  return meta;
}

BookCheck checked(BookCheck check) {
  if (check != BookCheck::KrakenChecksum && check != BookCheck::CoinbaseSequence) {
    throw std::invalid_argument("an order book's check is KrakenChecksum or CoinbaseSequence");
  }
  return check;
}

}  // namespace

OrderBook::OrderBook(const InstrumentMeta& instrument, BookCheck check,
                     std::uint32_t capacity_per_side)
    : meta_(checked(instrument)),
      check_(checked(check)),
      bids_(Side::Buy, capacity_per_side),
      asks_(Side::Sell, capacity_per_side) {
  static_assert(every_reason_has_a_counter(kReasonSlots),
                "every StaleReason needs a counter of its own (slot_of, kReasonSlots)");
  set_divisors();
}

// An update, stale or reset of another stream than the one the book is built
// from (ADR 0023). A snapshot of another source rebinds the book and an
// InstrumentUpdate applies from any source, so neither is one. Nor is a
// session start: a new session means every stream before it is over, so its
// reset applies from any source (the owner's second review of ADR 0023).
bool OrderBook::of_another_stream(const MarketEvent& event) const noexcept {
  const bool judged = event.kind == EventKind::BookDelta ||
                      event.kind == EventKind::StreamStale ||
                      (event.kind == EventKind::StreamReset &&
                       event.reason != StreamReason::SessionStart);
  return judged && bound_ && event.source != bound_source_;
}

BookResult OrderBook::apply(const MarketEvent& event) noexcept {
  if (event.instrument != meta_.id) {
    return {BookEffect::Ignored, StaleReason::NotStale};
  }
  if (of_another_stream(event)) {
    // Nothing of it is applied, and it interrupts nothing: an update counted
    // once, at its first level; a stale or reset counted as it comes.
    if (event.kind != EventKind::BookDelta) {
      ++other_source_controls_;
    } else if (event.level_index == 0) {
      ++other_source_updates_;
    }
    return {BookEffect::Ignored, StaleReason::NotStale};
  }
  if (progress_ == Progress::Idle) {
    return begin(event);
  }
  if (continues(event)) {
    return continue_message(event);
  }
  // Something else arrived before the message in progress was complete. The
  // message fails; the event is then taken on its own, and its result is the
  // step's if it found a failure of its own.
  const BookResult failed = fail(StaleReason::IncompleteMessage);
  const BookResult own = begin(event);
  return own.effect == BookEffect::WentStale ? own : failed;
}

// --- an event with no message in progress ----------------------------------------------------

BookResult OrderBook::begin(const MarketEvent& event) noexcept {
  switch (event.kind) {
    case EventKind::StreamStale:
      if (!reason_fits(EventKind::StreamStale, event.reason)) {
        return fail(StaleReason::InvalidEvent);
      }
      return fail(stale_reason_of(event.reason));
    case EventKind::StreamReset:
      if (!reason_fits(EventKind::StreamReset, event.reason)) {
        return fail(StaleReason::InvalidEvent);
      }
      clear_book();
      return {BookEffect::Reset, StaleReason::NotStale};
    case EventKind::InstrumentUpdate:
      return take_instrument(event);
    case EventKind::SnapshotBegin:
      start_message(Progress::FrameOpen, event);
      message_count_ = event.level_index;
      return {BookEffect::Pending, StaleReason::NotStale};
    case EventKind::BookDelta:
      if (event.level_index == 0) {
        return begin_update(event);
      }
      return orphan();
    case EventKind::BookSnapshot:
    case EventKind::BookCheckSnapshot:
    case EventKind::SnapshotEnd:
      return orphan();  // a snapshot's levels and end come only inside its frame
    case EventKind::Trade:
      return {BookEffect::Ignored, StaleReason::NotStale};
  }
  return fail(StaleReason::InvalidEvent);  // a kind the book does not know
}

// A level or frame end outside any message. While stale it is skipped, like
// any update; on a book that is not stale it means events were lost.
BookResult OrderBook::orphan() noexcept {
  if (stale_) {
    return {BookEffect::Skipped, StaleReason::NotStale};
  }
  return fail(StaleReason::IncompleteMessage);
}

BookResult OrderBook::begin_update(const MarketEvent& event) noexcept {
  if (stale_) {  // nothing is applied to a stale book, and nothing is checked
    return {BookEffect::Skipped, StaleReason::NotStale};
  }
  if (!has_book_) {
    return fail(StaleReason::DeltaBeforeSnapshot);
  }
  start_message(Progress::Update, event);
  // As while a snapshot is built, nothing can be read until the update takes
  // effect at its last level (ADR 0023): its levels apply to the live sides
  // one by one, and in between the book is one no venue had.
  has_book_ = false;
  return update_level(event);
}

BookResult OrderBook::take_instrument(const MarketEvent& event) noexcept {
  const InstrumentMeta next = instrument_meta(event);
  // Other exponents would give every mantissa in the book another meaning
  // (ADR 0019), and a tick or lot below 1 is no increment (ADR 0023); the
  // metadata in force stays.
  if (!meta_fits(next) || next.price_exp != meta_.price_exp || next.qty_exp != meta_.qty_exp) {
    return fail(StaleReason::InvalidEvent);
  }
  meta_ = next;
  set_divisors();
  return {BookEffect::Instrument, StaleReason::NotStale};
}

// --- the message in progress -----------------------------------------------------------------

void OrderBook::start_message(Progress progress, const MarketEvent& event) noexcept {
  progress_ = progress;
  message_source_ = event.source;
  message_seq_ = event.seq;
  message_count_ = 0;
  message_next_ = 0;
}

// The next level of a frame: in position, within the count, and flagged last
// exactly when it is the last.
bool OrderBook::next_level(const MarketEvent& event) const noexcept {
  return event.level_index == message_next_ && message_next_ < message_count_ &&
         event.has(MarketEvent::kLastInMessage) == (message_next_ + 1 == message_count_);
}

bool OrderBook::continues(const MarketEvent& event) const noexcept {
  if (event.source != message_source_ || event.seq != message_seq_) {
    return false;
  }
  switch (progress_) {
    case Progress::FrameOpen:
      if (event.kind == EventKind::SnapshotEnd) {  // only an empty frame ends here
        return message_count_ == 0 && event.level_index == 0;
      }
      return (event.kind == EventKind::BookSnapshot ||
              event.kind == EventKind::BookCheckSnapshot) &&
             next_level(event);
    case Progress::StreamFrame:
      return event.kind == EventKind::BookSnapshot && next_level(event);
    case Progress::CheckFrame:
      return event.kind == EventKind::BookCheckSnapshot && next_level(event);
    case Progress::FrameDone:
      return event.kind == EventKind::SnapshotEnd && event.level_index == message_count_;
    case Progress::Update:
      return event.kind == EventKind::BookDelta && event.level_index == message_next_;
    case Progress::Idle:
      return false;
  }
  return false;
}

BookResult OrderBook::continue_message(const MarketEvent& event) noexcept {
  switch (progress_) {
    case Progress::FrameOpen:
      if (event.kind == EventKind::SnapshotEnd) {
        // Nothing says whose snapshot it is, and no header verifies it (ADR 0022).
        return fail(StaleReason::InvalidEvent);
      }
      if (event.kind == EventKind::BookCheckSnapshot) {
        progress_ = Progress::CheckFrame;
        return check_level(event);
      }
      // A stream snapshot: the book is rebuilt from its levels, and cannot be
      // read until the frame ends. From here it is bound to the snapshot's
      // stream (ADR 0023).
      progress_ = Progress::StreamFrame;
      bids_.clear();
      asks_.clear();
      has_book_ = false;
      bound_ = true;
      bound_source_ = message_source_;
      return stream_level(event);
    case Progress::StreamFrame:
      return stream_level(event);
    case Progress::CheckFrame:
      return check_level(event);
    case Progress::FrameDone:
      progress_ = Progress::Idle;
      if (frame_is_check_) {
        return {BookEffect::Ignored, StaleReason::NotStale};
      }
      return finish(true);
    case Progress::Update:
      return update_level(event);
    case Progress::Idle:
      break;
  }
  return fail(StaleReason::InvalidEvent);
}

BookResult OrderBook::stream_level(const MarketEvent& event) noexcept {
  const StaleReason problem = apply_level(event);
  if (problem != StaleReason::NotStale) {
    return fail(problem);
  }
  ++message_next_;
  if (event.has(MarketEvent::kLastInMessage)) {
    take_header(event);
    frame_is_check_ = false;
    progress_ = Progress::FrameDone;
  }
  return {BookEffect::Pending, StaleReason::NotStale};
}

BookResult OrderBook::check_level(const MarketEvent& event) noexcept {
  ++message_next_;
  if (event.has(MarketEvent::kLastInMessage)) {
    frame_is_check_ = true;
    progress_ = Progress::FrameDone;
  }
  return {BookEffect::Pending, StaleReason::NotStale};
}

BookResult OrderBook::update_level(const MarketEvent& event) noexcept {
  const StaleReason problem = apply_level(event);
  if (problem != StaleReason::NotStale) {
    return fail(problem);
  }
  if (!event.has(MarketEvent::kLastInMessage)) {
    ++message_next_;
    return {BookEffect::Pending, StaleReason::NotStale};
  }
  take_header(event);
  progress_ = Progress::Idle;
  return finish(false);
}

StaleReason OrderBook::apply_level(const MarketEvent& event) noexcept {
  BookSide* side = event.side == Side::Buy ? &bids_ : event.side == Side::Sell ? &asks_ : nullptr;
  if (side == nullptr || event.qty.mantissa() < 0) {
    return StaleReason::InvalidEvent;
  }
  if (event.qty.mantissa() == 0) {
    side->remove(event.price);  // a level that is not there changes nothing
    return StaleReason::NotStale;
  }
  return side->set(event.price, event.qty) == BookSide::Set::Full ? StaleReason::CapacityExceeded
                                                                   : StaleReason::NotStale;
}

void OrderBook::take_header(const MarketEvent& event) noexcept {
  header_flags_ = event.flags;
  header_venue_seq_ = event.venue_seq;
  header_checksum_ = event.checksum;
  header_depth_ = event.depth;
}

// A message takes effect: the cut to its depth, then the venue's check.
BookResult OrderBook::finish(bool snapshot) noexcept {
  progress_ = Progress::Idle;
  if ((header_flags_ & MarketEvent::kHasDepth) != 0) {
    bids_.truncate(header_depth_);
    asks_.truncate(header_depth_);
  }
  if (snapshot) {
    has_last_venue_seq_ = false;  // a snapshot restarts the numbering
  }
  const StaleReason problem = verify();
  if (problem != StaleReason::NotStale) {
    return fail(problem);
  }
  has_book_ = true;
  if (stale_) {
    stale_ = false;
    stale_reason_ = StaleReason::NotStale;
    return {BookEffect::Recovered, StaleReason::NotStale};
  }
  return {BookEffect::Applied, StaleReason::NotStale};
}

StaleReason OrderBook::verify() noexcept {
  if (check_ == BookCheck::KrakenChecksum) {
    if ((header_flags_ & MarketEvent::kHasChecksum) == 0) {
      return StaleReason::InvalidEvent;  // nothing to verify the book by
    }
    return book_checksum() == header_checksum_ ? StaleReason::NotStale
                                               : StaleReason::ChecksumMismatch;
  }
  if ((header_flags_ & MarketEvent::kHasVenueSeq) == 0) {
    return StaleReason::InvalidEvent;
  }
  if (has_last_venue_seq_ && header_venue_seq_ <= last_venue_seq_) {
    return header_venue_seq_ == last_venue_seq_ ? StaleReason::SequenceRepeat
                                                : StaleReason::SequenceOutOfOrder;
  }
  has_last_venue_seq_ = true;
  last_venue_seq_ = header_venue_seq_;
  return StaleReason::NotStale;
}

// --- failure ------------------------------------------------------------------------------

BookResult OrderBook::fail(StaleReason reason) noexcept {
  const std::size_t slot = slot_of(reason);
  if (slot < counts_.size()) {
    ++counts_[slot];
  }
  if (!stale_) {
    stale_ = true;
    stale_reason_ = reason;
  }
  clear_book();
  progress_ = Progress::Idle;
  return {BookEffect::WentStale, reason};
}

void OrderBook::clear_book() noexcept {
  bids_.clear();
  asks_.clear();
  has_book_ = false;
  has_last_venue_seq_ = false;
  bound_ = false;  // no snapshot built what the book holds now: nothing
}

// --- queries ---------------------------------------------------------------------------------

std::uint64_t OrderBook::stale_count(StaleReason reason) const noexcept {
  const std::size_t slot = slot_of(reason);
  return slot < counts_.size() ? counts_[slot] : 0;
}

std::optional<std::uint32_t> OrderBook::bound_source() const noexcept {
  if (!bound_) {
    return std::nullopt;
  }
  return bound_source_;
}

const BookSide* OrderBook::side_of(Side side) const noexcept {
  if (side == Side::Buy) {
    return &bids_;
  }
  if (side == Side::Sell) {
    return &asks_;
  }
  return nullptr;
}

std::uint32_t OrderBook::top(Side side, std::span<BookLevel> out) const noexcept {
  const BookSide* levels = side_of(side);
  if (levels == nullptr || !has_book_) {
    return 0;
  }
  const auto count =
      static_cast<std::uint32_t>(std::min<std::size_t>(levels->size(), out.size()));
  for (std::uint32_t rank = 0; rank < count; ++rank) {
    out[rank] = (*levels)[rank];
  }
  return count;
}

std::optional<BookLevel> OrderBook::best_bid() const noexcept {
  if (!has_book_ || bids_.size() == 0) {
    return std::nullopt;
  }
  return bids_[0];
}

std::optional<BookLevel> OrderBook::best_ask() const noexcept {
  if (!has_book_ || asks_.size() == 0) {
    return std::nullopt;
  }
  return asks_[0];
}

std::uint32_t OrderBook::size(Side side) const noexcept {
  const BookSide* levels = side_of(side);
  return levels == nullptr || !has_book_ ? 0u : levels->size();
}

std::optional<Qty> OrderBook::quantity_at(Side side, Price price) const noexcept {
  const BookSide* levels = side_of(side);
  if (levels == nullptr || !has_book_) {
    return std::nullopt;
  }
  const BookLevel* level = levels->find(price);
  if (level == nullptr) {
    return std::nullopt;
  }
  return level->qty;
}

std::uint32_t OrderBook::checksum() const noexcept {
  return has_book_ ? book_checksum() : crc32(std::string_view{});  // an empty book's: 0
}

// Kraken's recipe (quantagent.data.book.kraken_checksum): the ten best asks,
// best first, then the ten best bids, best first; each level as the digits of
// its price then of its quantity, at the precision Kraken prints them. Of the
// sides as they are, readable or not: verify() runs it before the message
// takes effect.
std::uint32_t OrderBook::book_checksum() const noexcept {
  char text[2 * kChecksumLevels * 2 * 20];  // two sides, two numbers a level, 20 characters each
  char* out = text;
  for (const BookSide* side : {&asks_, &bids_}) {
    const std::uint32_t count = std::min(side->size(), kChecksumLevels);
    for (std::uint32_t rank = 0; rank < count; ++rank) {
      const BookLevel& level = (*side)[rank];
      out = write_digits(out, floor_divide(level.price.mantissa(), price_divisor_));
      out = write_digits(out, floor_divide(level.qty.mantissa(), qty_divisor_));
    }
  }
  return crc32_update(0, text, static_cast<std::size_t>(out - text));
}

void OrderBook::set_divisors() noexcept {
  price_divisor_ = kPowersOfTen[static_cast<std::size_t>(meta_.price_exp - meta_.price_precision)];
  qty_divisor_ = kPowersOfTen[static_cast<std::size_t>(meta_.qty_exp - meta_.qty_precision)];
}

}  // namespace qa
