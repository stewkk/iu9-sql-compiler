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
};

struct Projection {
  utils::NotNull<Group*> source;
  std::vector<Expression> expressions;
};

struct CrossJoin {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;
};

struct Join {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;
  using Type = JoinType;
  Type type;
  Expression qual;
};

} // namespace logical

struct LogicalExpr {
    std::variant<logical::Table, logical::Filter, logical::Projection, logical::Join, logical::CrossJoin> root_operator;
    utils::NotNull<Group*> group;
};

}  // namespace stewkk::sql
