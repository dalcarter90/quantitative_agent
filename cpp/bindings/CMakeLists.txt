# The extension is built against the interpreter in the repo's .venv (set via
# Python_EXECUTABLE in CMakePresets.json). pybind11 is pip-installed into that
# same venv, see docs/adr/0002-bindings-library.md.
find_package(Python 3.11 REQUIRED COMPONENTS Interpreter Development.Module)

execute_process(
  COMMAND "${Python_EXECUTABLE}" -m pybind11 --cmakedir
  OUTPUT_VARIABLE QA_PYBIND11_CMAKE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE QA_PYBIND11_RESULT)
if(NOT QA_PYBIND11_RESULT EQUAL 0)
  message(FATAL_ERROR
    "pybind11 not found for ${Python_EXECUTABLE}. "
    "Run: pip install -e \"python[dev]\" in the repo venv.")
endif()
set(pybind11_DIR "${QA_PYBIND11_CMAKE_DIR}")
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_core module.cpp)
target_link_libraries(_core PRIVATE qa_core qa_warnings)

# Emit the module straight into the Python package so an editable install
# picks it up. The generator expression stops multi-config generators from
# appending a per-config subdirectory.
set(QA_PYTHON_PACKAGE_DIR "${PROJECT_SOURCE_DIR}/../python/src/quantagent")
set_target_properties(_core PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "$<1:${QA_PYTHON_PACKAGE_DIR}>"
  RUNTIME_OUTPUT_DIRECTORY "$<1:${QA_PYTHON_PACKAGE_DIR}>")
