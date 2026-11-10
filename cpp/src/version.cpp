#include "qa/version.hpp"

namespace qa {

std::string_view version() noexcept { return QA_VERSION; }

std::string hello() { return "hello from qa_core " + std::string(version()); }

}  // namespace qa
