#include "qa/version.hpp"

#include <gtest/gtest.h>

TEST(Version, IsNonEmpty) { EXPECT_FALSE(qa::version().empty()); }

TEST(Version, HelloContainsVersion) {
  const std::string greeting = qa::hello();
  EXPECT_NE(greeting.find(qa::version()), std::string::npos);
}
