#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace stewkk::sql {

struct Attribute {
    std::string table;
    std::string name;

    auto operator<=>(const Attribute& other) const = default;
};

std::string ToString(const Attribute& attr);

using IntConst = std::int64_t;
using StringConst = std::string;

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
struct InExpression;
struct AggregateExpression;

enum class AggregateFunction {
    kSum,
    kCount,
};

std::string ToString(AggregateFunction function);

using Expression = std::variant<BinaryExpression, Attribute, IntConst, StringConst, UnaryExpression,
                                InExpression, AggregateExpression, Literal>;

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

struct InExpression {
    std::shared_ptr<Expression> lhs;
    std::vector<Expression> values;
    bool negated = false;

    bool operator==(const InExpression& other) const;
};

struct AggregateExpression {
    AggregateFunction function;
    std::shared_ptr<Expression> argument;
    bool is_star = false;

    bool operator==(const AggregateExpression& other) const;
};

}  // namespace stewkk::sql
