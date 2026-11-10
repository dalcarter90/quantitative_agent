find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(qa_tests test_version.cpp)
target_link_libraries(qa_tests PRIVATE qa_core qa_warnings GTest::gtest GTest::gtest_main)

gtest_discover_tests(qa_tests)
