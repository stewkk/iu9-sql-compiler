#pragma once

#include <memory>
#include <variant>
#include <vector>
#include <string>

namespace stewkk::sql {

constexpr static std::string kEmptyTableName = "_EMPTY_TABLE_";

struct Attribute {
    std::string table;
    std::string name;

    auto operator<=>(const Attribute& other) const = default;
};

std::string ToString(const Attribute& attr);

using IntConst = std::int64_t;

struct Table;
struct Projection;
struct Filter;
struct CrossJoin;
struct Join;

using Operator = std::variant<Table, Projection, Filter, CrossJoin, Join>;

struct Table {
    std::string name;

    auto operator<=>(const Table& other) const = default;
};

struct BinaryExpression;
struct UnaryExpression;

enum class Literal {
    kNull,
    kTrue,
    kFalse,
    kUnknown,
};

std::string ToString(Literal literal);

using Expression = std::variant<BinaryExpression, Attribute, IntConst, UnaryExpression, Literal>;

std::string ToString(const Expression& expr);

struct Projection {
    std::vector<Expression> expressions;
    std::shared_ptr<Operator> source;

    bool operator==(const Projection& other) const;
};

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

struct BinaryExpression {
    std::shared_ptr<Expression> lhs;
    BinaryOp binop;
    std::shared_ptr<Expression> rhs;

    bool operator==(const BinaryExpression& other) const;
};

enum class UnaryOp {
    kNot,
    kMinus,
};

std::string ToString(UnaryOp op);

struct UnaryExpression {
    UnaryOp op;
    std::shared_ptr<Expression> child;

    bool operator==(const UnaryExpression& other) const;
};

struct Filter {
    Expression expr;
    std::shared_ptr<Operator> source;

    bool operator==(const Filter& other) const;
};

struct CrossJoin {
    std::shared_ptr<Operator> lhs;
    std::shared_ptr<Operator> rhs;

    bool operator==(const CrossJoin& other) const;
};

enum class JoinType {
    kInner,
    kFull,
    kLeft,
    kRight,
};

std::string ToString(JoinType type);

struct Join {
    JoinType type;
    Expression qual;
    std::shared_ptr<Operator> lhs;
    std::shared_ptr<Operator> rhs;

    bool operator==(const Join& other) const;
};

}  // namespace stewkk::sql
