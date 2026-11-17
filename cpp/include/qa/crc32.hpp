#pragma once

// CRC-32 as zlib's crc32 computes it (IEEE 802.3: reflected, polynomial
// 0xEDB88320, initial and final value inverted). Kraken's book checksum is
// this CRC of the book's digits (ADR 0022). Table driven; no allocation.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace qa {

namespace detail {

[[nodiscard]] constexpr std::array<std::uint32_t, 256> make_crc32_table() noexcept {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t n = 0; n < 256; ++n) {
    std::uint32_t c = n;
    for (int bit = 0; bit < 8; ++bit) {
      c = (c & 1u) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
    }
    table[n] = c;
  }
  return table;
}

inline constexpr std::array<std::uint32_t, 256> kCrc32Table = make_crc32_table();

}  // namespace detail

// Continues `crc` (the CRC of what came before, 0 for nothing) over `size`
// more bytes, as zlib.crc32(data, crc) does.
[[nodiscard]] constexpr std::uint32_t crc32_update(std::uint32_t crc, const char* data,
                                                   std::size_t size) noexcept {
  crc = ~crc;
  for (std::size_t i = 0; i < size; ++i) {
    crc = detail::kCrc32Table[(crc ^ static_cast<std::uint8_t>(data[i])) & 0xFFu] ^ (crc >> 8);
  }
  return ~crc;
}

[[nodiscard]] constexpr std::uint32_t crc32(std::string_view text) noexcept {
  return crc32_update(0, text.data(), text.size());
}

}  // namespace qa
