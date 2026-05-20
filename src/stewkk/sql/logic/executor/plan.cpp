#include <stewkk/sql/logic/executor/plan.hpp>

namespace stewkk::sql {

bool PhysicalProjection::operator==(const PhysicalProjection& other) const {
  return *source == *other.source && expressions == other.expressions;
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
  return table == other.table && predicate == other.predicate;
}

} // namespace stewkk::sql
