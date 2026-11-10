#pragma once

#include <string>
#include <string_view>

namespace qa {

// Version of the C++ core, set from the CMake project version.
[[nodiscard]] std::string_view version() noexcept;

// Skeleton smoke-test function: proves the C++ -> Python path works end to end.
[[nodiscard]] std::string hello();

}  // namespace qa
