// One side of an order book: a fixed-capacity ring of levels kept best first
// (roadmap Phase 4b, ADR 0022).

#include "qa/book_side.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <random>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using qa::BookLevel;
using qa::BookSide;
using qa::Price;
using qa::Qty;
using qa::Side;

using Levels = std::vector<std::pair<std::int64_t, std::int64_t>>;

static_assert(sizeof(BookLevel) == 16 && std::is_trivially_copyable_v<BookLevel>);
static_assert(!std::is_copy_constructible_v<BookSide>);  // it owns its storage
static_assert(noexcept(std::declval<BookSide&>().set(Price(1), Qty(1))));
static_assert(noexcept(std::declval<BookSide&>().remove(Price(1))));

Levels levels_of(const BookSide& side) {
  Levels out;
  for (std::uint32_t rank = 0; rank < side.size(); ++rank) {
    out.emplace_back(side[rank].price.mantissa(), side[rank].qty.mantissa());
  }
  return out;
}

TEST(BookSide, BidsAreKeptHighestFirstAndAsksLowestFirst) {
  BookSide bids(Side::Buy, 8);
  BookSide asks(Side::Sell, 8);
  for (const std::int64_t price : {100, 98, 101, 99}) {
    EXPECT_EQ(bids.set(Price(price), Qty(price * 2)), BookSide::Set::Inserted);
    EXPECT_EQ(asks.set(Price(price), Qty(price * 2)), BookSide::Set::Inserted);
  }
  EXPECT_EQ(levels_of(bids), (Levels{{101, 202}, {100, 200}, {99, 198}, {98, 196}}));
  EXPECT_EQ(levels_of(asks), (Levels{{98, 196}, {99, 198}, {100, 200}, {101, 202}}));
  EXPECT_EQ(bids.size(), 4u);
  EXPECT_EQ(bids.capacity(), 8u);
}

TEST(BookSide, SettingAHeldPriceChangesItsQuantity) {
  BookSide bids(Side::Buy, 4);
  ASSERT_EQ(bids.set(Price(100), Qty(5)), BookSide::Set::Inserted);
  ASSERT_EQ(bids.set(Price(99), Qty(7)), BookSide::Set::Inserted);
  EXPECT_EQ(bids.set(Price(100), Qty(6)), BookSide::Set::Updated);
  EXPECT_EQ(levels_of(bids), (Levels{{100, 6}, {99, 7}}));
}

TEST(BookSide, RemovingALevelClosesTheGapAndAMissingOneChangesNothing) {
  BookSide asks(Side::Sell, 8);
  for (const std::int64_t price : {101, 102, 103, 104, 105}) {
    ASSERT_EQ(asks.set(Price(price), Qty(1)), BookSide::Set::Inserted);
  }
  EXPECT_TRUE(asks.remove(Price(103)));  // the middle
  EXPECT_TRUE(asks.remove(Price(101)));  // the best
  EXPECT_TRUE(asks.remove(Price(105)));  // the worst
  EXPECT_FALSE(asks.remove(Price(103)));
  EXPECT_FALSE(asks.remove(Price(999)));
  EXPECT_EQ(levels_of(asks), (Levels{{102, 1}, {104, 1}}));
}

TEST(BookSide, FindGivesTheLevelAtAPrice) {
  BookSide bids(Side::Buy, 8);
  EXPECT_EQ(bids.find(Price(100)), nullptr);
  ASSERT_EQ(bids.set(Price(100), Qty(5)), BookSide::Set::Inserted);
  ASSERT_EQ(bids.set(Price(98), Qty(7)), BookSide::Set::Inserted);
  ASSERT_NE(bids.find(Price(98)), nullptr);
  EXPECT_EQ(bids.find(Price(98))->qty, Qty(7));
  EXPECT_EQ(bids.find(Price(99)), nullptr);  // between two held prices
  EXPECT_EQ(bids.find(Price(101)), nullptr);
  EXPECT_EQ(bids.find(Price(97)), nullptr);
}

TEST(BookSide, ALevelBeyondTheCapacityIsRefusedAndNothingChanges) {
  BookSide bids(Side::Buy, 3);  // not a power of two: the ring is larger, the limit is 3
  for (const std::int64_t price : {100, 99, 98}) {
    ASSERT_EQ(bids.set(Price(price), Qty(1)), BookSide::Set::Inserted);
  }
  EXPECT_EQ(bids.set(Price(97), Qty(1)), BookSide::Set::Full);
  EXPECT_EQ(bids.set(Price(101), Qty(1)), BookSide::Set::Full);  // better than every level too
  EXPECT_EQ(levels_of(bids), (Levels{{100, 1}, {99, 1}, {98, 1}}));
  EXPECT_EQ(bids.set(Price(99), Qty(4)), BookSide::Set::Updated);  // a held price needs no room
  EXPECT_TRUE(bids.remove(Price(100)));
  EXPECT_EQ(bids.set(Price(97), Qty(1)), BookSide::Set::Inserted);
  EXPECT_EQ(levels_of(bids), (Levels{{99, 4}, {98, 1}, {97, 1}}));
}

TEST(BookSide, TruncateKeepsTheBestLevelsAndClearEmptiesIt) {
  BookSide bids(Side::Buy, 8);
  BookSide asks(Side::Sell, 8);
  for (const std::int64_t price : {5, 3, 1, 4, 2}) {
    ASSERT_EQ(bids.set(Price(price), Qty(price)), BookSide::Set::Inserted);
    ASSERT_EQ(asks.set(Price(price), Qty(price)), BookSide::Set::Inserted);
  }
  bids.truncate(2);
  asks.truncate(2);
  EXPECT_EQ(levels_of(bids), (Levels{{5, 5}, {4, 4}}));
  EXPECT_EQ(levels_of(asks), (Levels{{1, 1}, {2, 2}}));
  bids.truncate(7);  // deeper than it is: nothing to cut
  EXPECT_EQ(bids.size(), 2u);
  asks.truncate(0);
  EXPECT_EQ(asks.size(), 0u);
  bids.clear();
  EXPECT_EQ(bids.size(), 0u);
  EXPECT_EQ(bids.set(Price(9), Qty(9)), BookSide::Set::Inserted);
  EXPECT_EQ(levels_of(bids), (Levels{{9, 9}}));
}

TEST(BookSide, TheRingWrapsInBothDirections) {
  // Capacity 4 is a ring of 4 slots. Inserting at the best end moves the head
  // back past slot 0; removing the best moves it forward again, around the
  // ring several times.
  BookSide asks(Side::Sell, 4);
  std::int64_t best = 1'000;
  for (const std::int64_t price : {best, best + 1, best + 2}) {
    ASSERT_EQ(asks.set(Price(price), Qty(1)), BookSide::Set::Inserted);
  }
  for (int round = 0; round < 25; ++round) {
    --best;
    ASSERT_EQ(asks.set(Price(best), Qty(round + 2)), BookSide::Set::Inserted) << round;
    ASSERT_EQ(asks.size(), 4u);
    ASSERT_EQ(asks[0].price, Price(best));
    ASSERT_TRUE(asks.remove(Price(best + 3))) << round;  // the worst
    ASSERT_EQ(levels_of(asks).front().first, best);
    ASSERT_EQ(levels_of(asks).back().first, best + 2);
  }
  for (int round = 0; round < 25; ++round) {
    ASSERT_TRUE(asks.remove(Price(best))) << round;  // the best: the head moves forward
    ++best;
    ASSERT_EQ(asks.set(Price(best + 2), Qty(1)), BookSide::Set::Inserted) << round;
    ASSERT_EQ(asks[0].price, Price(best));
    ASSERT_EQ(asks[2].price, Price(best + 2));
  }
}

TEST(BookSide, ExtremePricesKeepTheirOrder) {
  constexpr auto kMin = std::numeric_limits<std::int64_t>::min();
  constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
  BookSide bids(Side::Buy, 8);
  for (const std::int64_t price : {std::int64_t{0}, kMin, kMax, std::int64_t{-1}}) {
    ASSERT_EQ(bids.set(Price(price), Qty(1)), BookSide::Set::Inserted);
  }
  EXPECT_EQ(levels_of(bids), (Levels{{kMax, 1}, {0, 1}, {-1, 1}, {kMin, 1}}));
}

TEST(BookSide, ACapacityOrSideItCannotHaveIsRefused) {
  EXPECT_THROW(BookSide(Side::Buy, 0), std::invalid_argument);
  EXPECT_THROW(BookSide(Side::Sell, BookSide::kMaxCapacity + 1), std::invalid_argument);
  EXPECT_THROW(BookSide(Side::Unknown, 8), std::invalid_argument);
  EXPECT_NO_THROW(BookSide(Side::Buy, 1));
}

// Against a std::map, over many random operations, at capacities that make the
// ring wrap constantly and at ones that never fill.
template <class Model>
void compare(const BookSide& side, const Model& model, std::uint32_t capacity, int step) {
  ASSERT_EQ(side.size(), model.size()) << step;
  ASSERT_LE(side.size(), capacity) << step;
  std::uint32_t rank = 0;
  for (const auto& [price, qty] : model) {
    ASSERT_EQ(side[rank].price, Price(price)) << step << " rank " << rank;
    ASSERT_EQ(side[rank].qty, Qty(qty)) << step << " rank " << rank;
    ++rank;
  }
}

template <class Model>
void run_random(Side which, std::uint32_t capacity, std::uint64_t seed) {
  BookSide side(which, capacity);
  Model model;
  std::mt19937_64 random(seed);
  const std::int64_t span = static_cast<std::int64_t>(capacity) * 2 + 3;
  for (int step = 0; step < 20'000; ++step) {
    const std::int64_t price =
        static_cast<std::int64_t>(random() % static_cast<std::uint64_t>(span)) - span / 2;
    const std::uint64_t action = random() % 16;
    if (action < 8) {
      const auto qty = static_cast<std::int64_t>(random() % 1'000 + 1);
      const bool held = model.count(price) != 0;
      const auto result = side.set(Price(price), Qty(qty));
      if (held) {
        ASSERT_EQ(result, BookSide::Set::Updated) << step;
        model[price] = qty;
      } else if (model.size() == capacity) {
        ASSERT_EQ(result, BookSide::Set::Full) << step;
      } else {
        ASSERT_EQ(result, BookSide::Set::Inserted) << step;
        model[price] = qty;
      }
    } else if (action < 14) {
      ASSERT_EQ(side.remove(Price(price)), model.erase(price) == 1) << step;
    } else if (action == 14) {
      const auto depth = static_cast<std::uint32_t>(random() % (capacity + 2));
      side.truncate(depth);
      while (model.size() > depth) {
        model.erase(std::prev(model.end()));
      }
    } else {
      const auto* found = side.find(Price(price));
      const auto it = model.find(price);
      ASSERT_EQ(found != nullptr, it != model.end()) << step;
      if (found != nullptr) {
        ASSERT_EQ(found->qty, Qty(it->second)) << step;
      }
      if (random() % 200 == 0) {
        side.clear();
        model.clear();
      }
    }
    compare(side, model, capacity, step);
    if (::testing::Test::HasFatalFailure()) {
      return;
    }
  }
}

TEST(BookSide, MatchesASortedMapOverRandomOperations) {
  std::uint64_t seed = 20261117;
  for (const std::uint32_t capacity : {1u, 2u, 3u, 7u, 8u, 9u, 64u, 100u}) {
    run_random<std::map<std::int64_t, std::int64_t, std::greater<>>>(Side::Buy, capacity, ++seed);
    ASSERT_FALSE(HasFatalFailure()) << "bids, capacity " << capacity;
    run_random<std::map<std::int64_t, std::int64_t>>(Side::Sell, capacity, ++seed);
    ASSERT_FALSE(HasFatalFailure()) << "asks, capacity " << capacity;
  }
}

}  // namespace
