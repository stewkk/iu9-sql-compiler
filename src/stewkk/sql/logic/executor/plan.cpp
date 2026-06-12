#include <stewkk/sql/logic/executor/plan.hpp>

namespace stewkk::sql {

bool PhysicalPlanNode::operator==(const PhysicalPlanNode& other) const {
  return node == other.node;
}

std::string_view OutputTable(const SeqScan& scan) {
  return scan.alias ? std::string_view{*scan.alias} : std::string_view{scan.table};
}

bool PhysicalProjection::operator==(const PhysicalProjection& other) const {
  return *source == *other.source && expressions == other.expressions && aliases == other.aliases;
}

bool PhysicalFilter::operator==(const PhysicalFilter& other) const {
  return *source == *other.source && predicate == other.predicate;
}

bool NestedLoopJoin::operator==(const NestedLoopJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs && type == other.type && qual == other.qual;
}

bool NestedLoopCrossJoin::operator==(const NestedLoopCrossJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs;
}

bool HashJoin::operator==(const HashJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs && type == other.type && qual == other.qual;
}

bool MergeJoin::operator==(const MergeJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs && type == other.type && qual == other.qual;
}

bool IndexSeek::operator==(const IndexSeek& other) const {
  return table == other.table && alias == other.alias && predicate == other.predicate
      && index_column == other.index_column;
}

bool PhysicalSort::operator==(const PhysicalSort& other) const {
  return *source == *other.source && keys == other.keys;
}

bool PhysicalAggregation::operator==(const PhysicalAggregation& other) const {
  return *source == *other.source && group_by == other.group_by && aggregates == other.aggregates;
}

bool PhysicalStreamAggregation::operator==(const PhysicalStreamAggregation& other) const {
  return *source == *other.source && group_by == other.group_by && aggregates == other.aggregates;
}

} // namespace stewkk::sql
