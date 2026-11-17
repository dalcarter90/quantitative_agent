#pragma once

// One side of an L2 order book (ADR 0022): its price levels, best first, in a
// ring buffer allocated once when it is built and never again.
//
// A level is found by binary search over its ranks. Inserting or removing one
// moves the levels on its nearer side by one slot, so a change near either end
// moves few: a snapshot sent best first appends at the worse end, and most
// updates are near the best. Cutting to a depth drops the worse end. A side
// holds at most `capacity` levels; a price it does not hold cannot be set when
// it is full.

#include "qa/types.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

namespace qa {

struct BookLevel {
  Price price;
  Qty qty;

  friend constexpr bool operator==(const BookLevel&, const BookLevel&) noexcept = default;
};

class BookSide {
 public:
  // Levels per side. 2^20 is a 16 MB ring; a Coinbase BTC-USD side is about
  // 23,000 levels.
  static constexpr std::uint32_t kMaxCapacity = 1u << 20;

  enum class Set : std::uint8_t {
    Inserted,  // a new level
    Updated,   // a held price got a new quantity
    Full,      // a new level, refused: the side holds `capacity` levels
  };

  // Throws std::invalid_argument for a side other than Buy or Sell, or a
  // capacity outside 1 to kMaxCapacity. Startup only.
  BookSide(Side side, std::uint32_t capacity)
      : side_(side),
        capacity_(capacity),
        mask_(ring_size(side, capacity) - 1),
        levels_(std::make_unique<BookLevel[]>(std::size_t{mask_} + 1)) {}

  BookSide(BookSide&&) noexcept = default;
  BookSide& operator=(BookSide&&) noexcept = default;
  BookSide(const BookSide&) = delete;
  BookSide& operator=(const BookSide&) = delete;
  ~BookSide() = default;

  [[nodiscard]] Side side() const noexcept { return side_; }
  [[nodiscard]] std::uint32_t capacity() const noexcept { return capacity_; }
  [[nodiscard]] std::uint32_t size() const noexcept { return size_; }

  // The level of rank `rank` (0 is the best), for rank < size().
  [[nodiscard]] const BookLevel& operator[](std::uint32_t rank) const noexcept {
    return levels_[slot(rank)];
  }

  [[nodiscard]] const BookLevel* find(Price price) const noexcept {
    const std::uint32_t rank = rank_of(price);
    if (rank < size_ && levels_[slot(rank)].price == price) {
      return &levels_[slot(rank)];
    }
    return nullptr;
  }

  // The level at `price` gets `qty`, inserted in order if the side does not
  // hold it and has room.
  [[nodiscard]] Set set(Price price, Qty qty) noexcept {
    const std::uint32_t rank = rank_of(price);
    if (rank < size_) {
      BookLevel& level = levels_[slot(rank)];
      if (level.price == price) {
        level.qty = qty;
        return Set::Updated;
      }
    }
    if (size_ == capacity_) {
      return Set::Full;
    }
    open(rank);
    levels_[slot(rank)] = BookLevel{price, qty};
    return Set::Inserted;
  }

  // Removes the level at `price`; false if the side does not hold it.
  bool remove(Price price) noexcept {
    const std::uint32_t rank = rank_of(price);
    if (rank >= size_ || levels_[slot(rank)].price != price) {
      return false;
    }
    close(rank);
    return true;
  }

  // Keeps the `depth` best levels.
  void truncate(std::uint32_t depth) noexcept {
    if (size_ > depth) {
      size_ = depth;
    }
  }

  void clear() noexcept {
    head_ = 0;
    size_ = 0;
  }

 private:
  static std::uint32_t ring_size(Side side, std::uint32_t capacity) {
    if (!is_book_side(side)) {
      throw std::invalid_argument("a book side is Buy or Sell");
    }
    if (capacity == 0 || capacity > kMaxCapacity) {
      throw std::invalid_argument("a book side's capacity must be 1 to " +
                                  std::to_string(kMaxCapacity) + " levels, not " +
                                  std::to_string(capacity));
    }
    return std::bit_ceil(capacity);
  }

  [[nodiscard]] std::uint32_t slot(std::uint32_t rank) const noexcept {
    return (head_ + rank) & mask_;
  }

  // Bids are best highest, asks best lowest.
  [[nodiscard]] bool better(Price a, Price b) const noexcept {
    return side_ == Side::Buy ? b < a : a < b;
  }

  // The first rank whose price is not better than `price`: its rank if the
  // side holds it, otherwise where it would go.
  [[nodiscard]] std::uint32_t rank_of(Price price) const noexcept {
    std::uint32_t low = 0;
    std::uint32_t high = size_;
    while (low < high) {
      const std::uint32_t middle = low + (high - low) / 2;
      if (better(levels_[slot(middle)].price, price)) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  // Room at `rank`, for size() < capacity(): the levels on its nearer side
  // move one slot outward.
  void open(std::uint32_t rank) noexcept {
    if (rank < size_ - rank) {  // the better levels, towards the front
      head_ = (head_ - 1) & mask_;
      for (std::uint32_t r = 0; r < rank; ++r) {
        levels_[slot(r)] = levels_[slot(r + 1)];
      }
    } else {  // the worse levels, towards the back
      for (std::uint32_t r = size_; r > rank; --r) {
        levels_[slot(r)] = levels_[slot(r - 1)];
      }
    }
    ++size_;
  }

  // Closes the gap at `rank`, for rank < size(), from its nearer side.
  void close(std::uint32_t rank) noexcept {
    if (rank < size_ - 1 - rank) {
      for (std::uint32_t r = rank; r > 0; --r) {
        levels_[slot(r)] = levels_[slot(r - 1)];
      }
      head_ = (head_ + 1) & mask_;
    } else {
      for (std::uint32_t r = rank; r + 1 < size_; ++r) {
        levels_[slot(r)] = levels_[slot(r + 1)];
      }
    }
    --size_;
  }

  Side side_;
  std::uint32_t capacity_;
  std::uint32_t mask_;  // the ring's size, a power of two at least the capacity, minus one
  std::unique_ptr<BookLevel[]> levels_;
  std::uint32_t head_ = 0;  // the slot of rank 0
  std::uint32_t size_ = 0;
};

}  // namespace qa
