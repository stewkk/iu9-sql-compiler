#pragma once

#include <variant>
#include <vector>
#include <optional>

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {

struct Group;

namespace physical {

struct SeqScan {
  std::string table;
  std::optional<std::string> alias;

  bool operator==(const SeqScan&) const = default;
};

struct IndexSeek {
  std::string table;
  std::optional<std::string> alias;
  Expression predicate;
  std::string index_column;

  bool operator==(const IndexSeek&) const = default;
};

struct Projection {
  utils::NotNull<Group*> source;
  std::vector<Expression> expressions;
  std::vector<std::optional<std::string>> aliases;

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

struct HashJoin {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;
  JoinType type;
  Expression qual;

  bool operator==(const HashJoin&) const = default;
};

struct MergeJoin {
  utils::NotNull<Group*> lhs;
  utils::NotNull<Group*> rhs;
  JoinType type;
  Expression qual;

  bool operator==(const MergeJoin&) const = default;
};

struct Sort {
  utils::NotNull<Group*> input;
  SortOrder keys;

  bool operator==(const Sort&) const = default;
};

struct Aggregation {
  utils::NotNull<Group*> source;
  std::vector<Expression> group_by;
  std::vector<Expression> aggregates;

  bool operator==(const Aggregation&) const = default;
};

struct StreamAggregation {
  utils::NotNull<Group*> source;
  std::vector<Expression> group_by;
  std::vector<Expression> aggregates;

  bool operator==(const StreamAggregation&) const = default;
};

struct PartialAggregation {
  utils::NotNull<Group*> source;
  std::vector<Expression> group_by;
  std::vector<Expression> aggregates;

  bool operator==(const PartialAggregation&) const = default;
};

struct FinalAggregation {
  utils::NotNull<Group*> source;
  std::vector<Expression> group_by;
  std::vector<Expression> aggregates;

  bool operator==(const FinalAggregation&) const = default;
};

} // namespace physical

struct PhysicalExpr {
    std::variant<physical::SeqScan, physical::Projection, physical::Filter,
                 physical::NestedLoopJoin, physical::NestedLoopCrossJoin,
                 physical::HashJoin, physical::MergeJoin, physical::Sort,
                 physical::Aggregation, physical::StreamAggregation,
                 physical::PartialAggregation, physical::FinalAggregation,
                 physical::IndexSeek> root_operator;
    utils::NotNull<Group*> group;
    bool is_enforcer = false;
};

}  // namespace stewkk::sql
