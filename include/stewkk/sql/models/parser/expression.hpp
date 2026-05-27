#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <variant>

namespace stewkk::sql {

struct Attribute {
    std::string table;
    std::string name;

    auto operator<=>(const Attribute& other) const = default;
};

std::string ToString(const Attribute& attr);

using IntConst = std::int64_t;

enum class Literal {
    kNull,
    kTrue,
    kFalse,
    kUnknown,
};

std::string ToString(Literal literal);

enum class BinaryOp {
   kGt,
   kLt,
   kLe,
   kGe,
   kNotEq,
   kEq,
   kOr,
   kAnd,
   kPlus,
   kMinus,
   kMul,
   kDiv,
   kMod,
   kPow,
};

std::string ToString(BinaryOp binop);

enum class UnaryOp {
    kNot,
    kMinus,
    kIsNull,
};

std::string ToString(UnaryOp op);

struct BinaryExpression;
struct UnaryExpression;

using Expression = std::variant<BinaryExpression, Attribute, IntConst, UnaryExpression, Literal>;

std::string ToString(const Expression& expr);

struct BinaryExpression {
    std::shared_ptr<Expression> lhs;
    BinaryOp binop;
    std::shared_ptr<Expression> rhs;

    bool operator==(const BinaryExpression& other) const;
};

struct UnaryExpression {
    UnaryOp op;
    std::shared_ptr<Expression> child;

    bool operator==(const UnaryExpression& other) const;
};

}  // namespace stewkk::sql
