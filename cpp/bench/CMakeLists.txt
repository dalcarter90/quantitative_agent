find_package(benchmark CONFIG REQUIRED)

add_executable(qa_bench bench_version.cpp bench_notional.cpp bench_order_book.cpp)
# The order book benchmark generates its streams with the tests' event builders.
target_include_directories(qa_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../tests)
target_link_libraries(qa_bench PRIVATE qa_core qa_warnings benchmark::benchmark benchmark::benchmark_main)
