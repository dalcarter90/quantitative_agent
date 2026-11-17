// CRC-32 as zlib computes it (the checksum Kraken's book rule uses).

#include "qa/crc32.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

namespace {

// Known values: the standard check value, and zlib.crc32 of each text.
static_assert(qa::crc32("") == 0u);
static_assert(qa::crc32("123456789") == 0xCBF43926u);

TEST(Crc32, MatchesKnownValues) {
  EXPECT_EQ(qa::crc32(""), 0u);
  EXPECT_EQ(qa::crc32("a"), 0xE8B7BE43u);
  EXPECT_EQ(qa::crc32("abc"), 0x352441C2u);
  EXPECT_EQ(qa::crc32("123456789"), 0xCBF43926u);
  EXPECT_EQ(qa::crc32("The quick brown fox jumps over the lazy dog"), 0x414FA339u);
}

TEST(Crc32, TextCanBeFedInPieces) {
  const std::string_view text = "73468689472056734700208204";
  const std::uint32_t whole = qa::crc32(text);
  for (std::size_t cut = 0; cut <= text.size(); ++cut) {
    std::uint32_t crc = qa::crc32_update(0, text.data(), cut);
    crc = qa::crc32_update(crc, text.data() + cut, text.size() - cut);
    EXPECT_EQ(crc, whole) << cut;
  }
  EXPECT_EQ(qa::crc32_update(0x12345678u, text.data(), 0), 0x12345678u);
}

TEST(Crc32, EveryByteValueCounts) {
  std::string bytes;
  for (int value = 0; value < 256; ++value) {
    bytes.push_back(static_cast<char>(value));
  }
  EXPECT_EQ(qa::crc32(bytes), 0x29058C73u);  // zlib.crc32(bytes(range(256)))
}

}  // namespace
