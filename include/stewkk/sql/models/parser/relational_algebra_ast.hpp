#pragma once

#include <memory>
#include <optional>
#include <variant>
#include <vector>
#include <string>
#include <string_view>

#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/models/parser/join_type.hpp>

namespace stewkk::sql {

constexpr static std::string kEmptyTableName = "_EMPTY_TABLE_";

struct Table;
struct Projection;
struct Filter;
struct CrossJoin;
struct Join;

using Operator = std::variant<Table, Projection, Filter, CrossJoin, Join>;

struct Table {
    std::string name;
    std::optional<std::string> alias;

    auto operator<=>(const Table& other) const = default;
};

std::string_view VisibleName(const Table& table);

struct Projection {
    std::vector<Expression> expressions;
    std::shared_ptr<Operator> source;

    bool operator==(const Projection& other) const;
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

struct Join {
    JoinType type;
    Expression qual;
    std::shared_ptr<Operator> lhs;
    std::shared_ptr<Operator> rhs;

    bool operator==(const Join& other) const;
};

}  // namespace stewkk::sql
