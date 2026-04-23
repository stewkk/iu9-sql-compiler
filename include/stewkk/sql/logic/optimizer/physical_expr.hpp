#pragma once

#include <variant>
#include <vector>

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

struct Group;

namespace physical {

struct SeqScan {
  std::string table;

  bool operator==(const SeqScan&) const = default;
};

struct Projection {
  utils::NotNull<Group*> source;
  std::vector<Expression> expressions;

  bool operator==(const Projection&) const = default;
};

struct Filter {
  utils::NotNull<Group*> source;
  Expression predicate;

  bool operator==(const Filter&) const = default;
};

struct NestedLoopJoin {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;
  JoinType type;
  Expression qual;

  bool operator==(const NestedLoopJoin&) const = default;
};

struct NestedLoopCrossJoin {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;

  bool operator==(const NestedLoopCrossJoin&) const = default;
};

} // namespace physical

struct PhysicalExpr {
    std::variant<physical::SeqScan, physical::Projection, physical::Filter,
                 physical::NestedLoopJoin, physical::NestedLoopCrossJoin> root_operator;
    utils::NotNull<Group*> group;
};

}  // namespace stewkk::sql
