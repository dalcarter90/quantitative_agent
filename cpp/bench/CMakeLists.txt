find_package(benchmark CONFIG REQUIRED)

add_executable(qa_bench bench_version.cpp bench_notional.cpp)
target_link_libraries(qa_bench PRIVATE qa_core qa_warnings benchmark::benchmark benchmark::benchmark_main)
