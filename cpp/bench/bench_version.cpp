#include "qa/version.hpp"

#include <benchmark/benchmark.h>

// Placeholder benchmark: proves Google Benchmark is wired up. Real benchmarks
// (order book update, SPSC queue) arrive with their components.
static void BM_Hello(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(qa::hello());
  }
}
BENCHMARK(BM_Hello);
