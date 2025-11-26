#include <stewkk/sql/logic/result/error.hpp>

#include <ranges>
#include <sstream>

namespace stewkk::sql {

namespace {
const static std::string kWhat = "no data";
}

Error::Error(ErrorType type, std::string message)
    : wrapped_({ErrorData{std::move(type), std::move(message)}}) {}

std::string Error::What() const {
  std::ostringstream res;
  res << wrapped_.back().message;
  auto view_without_first = wrapped_ | std::views::reverse | std::views::drop(1);
  for (const auto& el : view_without_first) {
    res << ": " << el.message;
  }
  return res.str();
}

Error& Error::Wrap(ErrorType type, std::string message) {
  wrapped_.emplace_back(std::move(type), std::move(message));
  return *this;
}

bool Error::Wraps(ErrorType type) const {
  return std::ranges::find(wrapped_, type, &ErrorData::type) != wrapped_.end();
}

const char* Error::what() const noexcept { return kWhat.c_str(); }

}  // namespace stewkk::sql
