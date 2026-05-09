#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

bool Projection::operator==(const Projection& other) const {
  return expressions == other.expressions && *source == *other.source;
}

bool Filter::operator==(const Filter& other) const {
  return expr == other.expr && *source == *other.source;
}

bool CrossJoin::operator==(const CrossJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs;
}

bool Join::operator==(const Join& other) const {
  return type == other.type && qual == other.qual && *lhs == *other.lhs && *rhs == *other.rhs;
}

} // namespace stewkk::sql
