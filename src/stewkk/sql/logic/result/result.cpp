#include <stewkk/sql/logic/result/result.hpp>

namespace stewkk::sql {

std::string What(const Error& error) { return error.What(); }

}  // namespace stewkk::sql
