#pragma once

#include <istream>
#include <memory>
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

struct Table;
struct Projection;
struct Filter;

using Operator = std::variant<Table, Projection, Filter>;

struct Table {
    std::string name;

    auto operator<=>(const Table& other) const = default;
};

struct Projection {
    std::vector<Attribute> attributes;
    std::shared_ptr<Operator> source;

    bool operator==(const Projection& other) const;
};

struct BinaryExpression;

using Expression = std::variant<BinaryExpression, Attribute, IntConst>;

enum class BinaryOp {
   kGt,
};

std::string ToString(BinaryOp binop);

struct BinaryExpression {
    std::shared_ptr<Expression> lhs;
    BinaryOp binop;
    std::shared_ptr<Expression> rhs;

    bool operator==(const BinaryExpression& other) const;
};

struct Filter {
    Expression expr;
    std::shared_ptr<Operator> source;

    bool operator==(const Filter& other) const;
};

Operator GetAST(std::istream& in);

std::string GetDotRepresentation(const Operator& op);

}  // namespace stewkk::sql
