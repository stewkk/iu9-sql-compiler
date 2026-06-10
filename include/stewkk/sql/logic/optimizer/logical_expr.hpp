#pragma once

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

struct Group;

namespace logical {

using Table = Table;

struct Filter {
  utils::NotNull<Group*> source;
  Expression predicate;

  bool operator==(const Filter&) const = default;
};

struct Projection {
  utils::NotNull<Group*> source;
  std::vector<Expression> expressions;
  std::vector<std::optional<std::string>> aliases;

  bool operator==(const Projection&) const = default;
};

struct Aggregation {
  utils::NotNull<Group*> source;
  std::vector<Expression> group_by;
  std::vector<Expression> aggregates;

  bool operator==(const Aggregation&) const = default;
};

struct CrossJoin {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;

  bool operator==(const CrossJoin&) const = default;
};

struct Join {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;
  using Type = JoinType;
  Type type;
  Expression qual;

  bool operator==(const Join&) const = default;
};

} // namespace logical

struct LogicalExpr {
    std::variant<logical::Table, logical::Filter, logical::Projection, logical::Aggregation,
                 logical::Join, logical::CrossJoin> root_operator;
    utils::NotNull<Group*> group;
};

}  // namespace stewkk::sql
