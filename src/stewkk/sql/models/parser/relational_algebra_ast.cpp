#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

bool Projection::operator==(const Projection& other) const {
  return attributes == other.attributes && *source == *other.source;
}

bool Filter::operator==(const Filter& other) const {
  return expr == other.expr && *source == *other.source;
}

bool BinaryExpression::operator==(const BinaryExpression& other) const {
  return *lhs == *other.lhs && binop == other.binop && *rhs == *other.rhs;
}

std::string ToString(BinaryOp binop) {
    switch (binop) {
      case BinaryOp::kGt:
        return ">";
    }
}

std::string ToString(const Attribute& attr) {
  return std::format("{}.{}", attr.table, attr.name);
}

} // namespace stewkk::sql
