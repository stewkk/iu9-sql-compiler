#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

bool Projection::operator==(const Projection& other) const {
  return expressions == other.expressions && *source == *other.source;
}

bool Filter::operator==(const Filter& other) const {
  return expr == other.expr && *source == *other.source;
}

bool BinaryExpression::operator==(const BinaryExpression& other) const {
  return *lhs == *other.lhs && binop == other.binop && *rhs == *other.rhs;
}

bool UnaryExpression::operator==(const UnaryExpression& other) const {
  return op == other.op && *child == *other.child;
}

bool CrossJoin::operator==(const CrossJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs;
}

std::string ToString(BinaryOp binop) {
    switch (binop) {
      case BinaryOp::kGt:
        return ">";
      case BinaryOp::kOr:
        return "or";
      case BinaryOp::kAnd:
        return "and";
      case BinaryOp::kEq:
        return "=";
      case BinaryOp::kPlus:
        return "+";
      case BinaryOp::kMinus:
        return "-";
      case BinaryOp::kMul:
        return "*";
      case BinaryOp::kDiv:
        return "/";
      case BinaryOp::kMod:
        return "%";
      case BinaryOp::kPow:
        return "^";
    }
}

std::string ToString(const Attribute& attr) {
  return std::format("{}.{}", attr.table, attr.name);
}

std::string ToString(UnaryOp op) {
    switch (op) {
      case UnaryOp::kNot:
        return "not";
    }
}

std::string ToString(Literal literal) {
  switch (literal) {
    case Literal::kNull:
      return "NULL";
    case Literal::kTrue:
      return "TRUE";
    case Literal::kFalse:
      return "FALSE";
    case Literal::kUnknown:
      return "UNKNOWN";
  }
}

std::string ToString(const Expression& expr) {
    struct Formatter {
        std::string operator()(const BinaryExpression& expr) {
          return std::format("{} {} {}", ToString(*expr.lhs), ToString(expr.binop), ToString(*expr.rhs));
        }
        std::string operator()(const Attribute& expr) {
          return ToString(expr);
        }
        std::string operator()(const IntConst& expr) {
          return std::to_string(expr);
        }
        std::string operator()(const UnaryExpression& expr) {
          return std::format("{} {}", ToString(expr.op), ToString(*expr.child));
        }
        std::string operator()(const Literal& expr) {
          return ToString(expr);
        }
    };
    return std::visit(Formatter{}, expr);
}

} // namespace stewkk::sql
