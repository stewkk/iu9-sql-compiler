#pragma once

#include <optional>
#include <string>
#include <vector>

namespace stewkk::sql {

enum class ErrorType {
  kUnknown,
  kNotFound,
  kNotMaster,
  kNotConnected,
};

class Error : public std::exception {
private:
  struct ErrorData {
    ErrorType type;
    std::string message;
  };

public:
  Error(ErrorType type, std::string message);
  std::string What() const;
  virtual const char* what() const noexcept override;
  Error& Wrap(ErrorType type, std::string message);
  bool Wraps(ErrorType type) const;

private:
  std::vector<ErrorData> wrapped_;
  std::optional<std::string> what_;
};

}  // namespace stewkk::sql
