#pragma once

#include <string>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/models/parser/join_type.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {

struct SeqScan;
struct PhysicalProjection;
struct PhysicalFilter;
struct NestedLoopJoin;
struct NestedLoopCrossJoin;
struct HashJoin;
struct MergeJoin;
struct IndexSeek;
struct PhysicalSort;
struct PhysicalAggregation;

using PhysicalPlanNode = std::variant<SeqScan, PhysicalProjection, PhysicalFilter, NestedLoopCrossJoin, NestedLoopJoin, HashJoin, MergeJoin, IndexSeek, PhysicalSort, PhysicalAggregation>;

struct SeqScan {
  std::string table;
  std::optional<std::string> alias;

  bool operator==(const SeqScan&) const = default;
};

std::string_view OutputTable(const SeqScan& scan);

struct PhysicalProjection {
  std::shared_ptr<PhysicalPlanNode> source;
  std::vector<Expression> expressions;
  std::vector<std::optional<std::string>> aliases;

  bool operator==(const PhysicalProjection&) const;
};

struct PhysicalFilter {
  std::shared_ptr<PhysicalPlanNode> source;
  Expression predicate;

  bool operator==(const PhysicalFilter&) const;
};

struct NestedLoopJoin {
  std::shared_ptr<PhysicalPlanNode> lhs;
  std::shared_ptr<PhysicalPlanNode> rhs;
  JoinType type;
  Expression qual;

  bool operator==(const NestedLoopJoin&) const;
};

struct NestedLoopCrossJoin {
  std::shared_ptr<PhysicalPlanNode> lhs;
  std::shared_ptr<PhysicalPlanNode> rhs;

  bool operator==(const NestedLoopCrossJoin&) const;
};

struct HashJoin {
  std::shared_ptr<PhysicalPlanNode> lhs;
  std::shared_ptr<PhysicalPlanNode> rhs;
  JoinType type;
  Expression qual;

  bool operator==(const HashJoin&) const;
};

struct MergeJoin {
  std::shared_ptr<PhysicalPlanNode> lhs;
  std::shared_ptr<PhysicalPlanNode> rhs;
  JoinType type;
  Expression qual;

  bool operator==(const MergeJoin&) const;
};

struct IndexSeek {
  std::string table;
  std::optional<std::string> alias;
  Expression predicate;

  bool operator==(const IndexSeek&) const;
};

struct PhysicalSort {
  std::shared_ptr<PhysicalPlanNode> source;
  SortOrder keys;

  bool operator==(const PhysicalSort&) const;
};

struct PhysicalAggregation {
  std::shared_ptr<PhysicalPlanNode> source;
  std::vector<Expression> group_by;
  std::vector<Expression> aggregates;

  bool operator==(const PhysicalAggregation&) const;
};

} // namespace stewkk::sql
