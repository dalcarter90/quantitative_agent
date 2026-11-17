find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(qa_tests
  test_version.cpp
  test_types.cpp
  test_notional.cpp
  test_events.cpp
  test_clock.cpp
  test_crc32.cpp
  test_book_side.cpp
  test_order_book.cpp)
target_link_libraries(qa_tests PRIVATE qa_core qa_warnings GTest::gtest GTest::gtest_main)

gtest_discover_tests(qa_tests)

# Replaces the global operator new to count allocations, so it is its own
# binary: nothing else runs on the counting allocator (roadmap Phase 4b).
add_executable(qa_alloc_tests test_order_book_alloc.cpp)
target_link_libraries(qa_alloc_tests PRIVATE qa_core qa_warnings GTest::gtest GTest::gtest_main)

gtest_discover_tests(qa_alloc_tests)
