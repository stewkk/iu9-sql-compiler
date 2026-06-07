#pragma once

#include <string>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
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
struct PhysicalPlanNode;

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

struct PlanNodeMetadata {
  std::int64_t cardinality = 0;
  std::int64_t local_cost = 0;

  bool operator==(const PlanNodeMetadata&) const = default;
};

using PhysicalPlanAlternative = std::variant<SeqScan, PhysicalProjection, PhysicalFilter, NestedLoopCrossJoin, NestedLoopJoin, HashJoin, MergeJoin, IndexSeek, PhysicalSort, PhysicalAggregation>;

struct PhysicalPlanNode {
  PhysicalPlanAlternative node;
  std::optional<PlanNodeMetadata> metadata;

  PhysicalPlanNode() = default;

  template <typename T>
    requires (!std::same_as<std::remove_cvref_t<T>, PhysicalPlanNode>
              && std::constructible_from<PhysicalPlanAlternative, T>)
  PhysicalPlanNode(T&& value) : node(std::forward<T>(value)) {}

  bool operator==(const PhysicalPlanNode& other) const;
};

} // namespace stewkk::sql
