cmake_minimum_required(VERSION 3.25)

project(quantagent VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(QA_BUILD_TESTS "Build GoogleTest unit tests" ON)
option(QA_BUILD_BENCHMARKS "Build Google Benchmark binaries" ON)
option(QA_BUILD_PYTHON "Build the pybind11 extension module" ON)

# Warnings are errors for our own targets only; third-party code is untouched.
add_library(qa_warnings INTERFACE)
if(MSVC)
  target_compile_options(qa_warnings INTERFACE /W4 /WX /permissive- /utf-8)
else()
  target_compile_options(qa_warnings INTERFACE -Wall -Wextra -Wpedantic -Werror)
endif()

add_library(qa_core STATIC src/version.cpp)
target_include_directories(qa_core PUBLIC include)
target_compile_definitions(qa_core PRIVATE QA_VERSION="${PROJECT_VERSION}")
target_link_libraries(qa_core PRIVATE qa_warnings)
# Linked into the Python extension module, so it must be position independent.
set_target_properties(qa_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(QA_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(QA_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()

if(QA_BUILD_PYTHON)
  add_subdirectory(bindings)
endif()
