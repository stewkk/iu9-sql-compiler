#include <stewkk/sql/models/parser/expression.hpp>

#include <format>
#include <ranges>

namespace stewkk::sql {

bool BinaryExpression::operator==(const BinaryExpression& other) const {
  return *lhs == *other.lhs && binop == other.binop && *rhs == *other.rhs;
}

bool UnaryExpression::operator==(const UnaryExpression& other) const {
  return op == other.op && *child == *other.child;
}

bool InExpression::operator==(const InExpression& other) const {
  return *lhs == *other.lhs && values == other.values && negated == other.negated;
}

bool AggregateExpression::operator==(const AggregateExpression& other) const {
  if (function != other.function || is_star != other.is_star) {
    return false;
  }
  if (is_star) {
    return true;
  }
  return argument && other.argument && *argument == *other.argument;
}

std::string ToString(AggregateFunction function) {
  switch (function) {
    case AggregateFunction::kSum:
      return "SUM";
    case AggregateFunction::kCount:
      return "COUNT";
  }
}

std::string ToString(const Attribute& attr) {
  return std::format("{}.{}", attr.table, attr.name);
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
      case BinaryOp::kLt:
        return "<";
      case BinaryOp::kLe:
        return "<=";
      case BinaryOp::kGe:
        return ">=";
      case BinaryOp::kNotEq:
        return "!=";
    }
}

std::string ToString(UnaryOp op) {
    switch (op) {
      case UnaryOp::kNot:
        return "not";
      case UnaryOp::kMinus:
        return "-";
      case UnaryOp::kIsNull:
        return "isnull";
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
        std::string operator()(const StringConst& expr) {
          std::string escaped;
          escaped.reserve(expr.size() + 2);
          escaped.push_back('\'');
          for (char c : expr) {
            if (c == '\'') escaped += "''";
            else escaped.push_back(c);
          }
          escaped.push_back('\'');
          return escaped;
        }
        std::string operator()(const UnaryExpression& expr) {
          return std::format("{} {}", ToString(expr.op), ToString(*expr.child));
        }
        std::string operator()(const InExpression& expr) {
          auto values = expr.values | std::views::transform([](const Expression& v) {
                          return ToString(v);
                        }) | std::views::join_with(',') | std::ranges::to<std::string>();
          return std::format("{} {}in ({})", ToString(*expr.lhs), expr.negated ? "not " : "", values);
        }
        std::string operator()(const AggregateExpression& expr) {
          if (expr.is_star) {
            return std::format("{}(*)", ToString(expr.function));
          }
          return std::format("{}({})", ToString(expr.function), ToString(*expr.argument));
        }
        std::string operator()(const Literal& expr) {
          return ToString(expr);
        }
    };
    return std::visit(Formatter{}, expr);
}

} // namespace stewkk::sql
