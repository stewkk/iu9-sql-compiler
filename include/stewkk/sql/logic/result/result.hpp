#pragma once

#include <expected>
#include <format>

#include <stewkk/sql/logic/result/error.hpp>

namespace stewkk::sql {

struct EmptyResult {};

template <typename OkResult = EmptyResult> using Result = std::expected<OkResult, Error>;

template <typename T = EmptyResult> auto Ok(T&& v = {}) { return Result<T>(std::forward<T>(v)); }

template <ErrorType ErrorType = ErrorType::kUnknown, typename... Args>
std::unexpected<Error> MakeError(const std::format_string<Args...> fmt, Args&&... args) {
  return std::unexpected<Error>(Error{ErrorType, std::format(fmt, std::forward<Args>(args)...)});
}

template <ErrorType ErrorType = ErrorType::kUnknown, typename T, typename... Args>
std::unexpected<Error> WrapError(Result<T>&& result, const std::format_string<Args...> fmt, Args&&... args) {
  return std::unexpected<Error>(result.error().Wrap(ErrorType, std::format(fmt, std::forward<Args>(args)...)));
}

std::string What(const Error& error);

} // namespace stewkk::sql
