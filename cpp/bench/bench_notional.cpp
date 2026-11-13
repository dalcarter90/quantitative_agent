#include "qa/notional.hpp"

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

namespace {

std::vector<std::int64_t> operands() {
  std::mt19937_64 random(7);
  std::vector<std::int64_t> values(4096);
  for (auto& value : values) {
    value = static_cast<std::int64_t>(random());
  }
  return values;
}

void BM_Notional(benchmark::State& state) {
  const auto values = operands();
  std::size_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(
        qa::notional(qa::Price(values[i & 4095]), qa::Qty(values[(i + 1) & 4095])));
    ++i;
  }
}
BENCHMARK(BM_Notional);

void BM_MultiplyPortable(benchmark::State& state) {
  const auto values = operands();
  std::size_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(qa::multiply_portable(values[i & 4095], values[(i + 1) & 4095]));
    ++i;
  }
}
BENCHMARK(BM_MultiplyPortable);

}  // namespace
