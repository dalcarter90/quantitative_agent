#include "qa/version.hpp"

#include <pybind11/pybind11.h>

#include <string>

PYBIND11_MODULE(_core, m) {
  m.doc() = "quantagent C++ core bindings";

  m.def("version", [] { return std::string(qa::version()); }, "Version of the C++ core.");
  m.def("hello", &qa::hello, "Skeleton smoke test for the C++ -> Python path.");
}
