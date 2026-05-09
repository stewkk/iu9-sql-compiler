#include <string>
#include <variant>
#include <vector>

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/models/parser/join_type.hpp>

namespace stewkk::sql {

struct SeqScan;
struct PhysicalProjection;
struct PhysicalFilter;
struct NestedLoopJoin;
struct NestedLoopCrossJoin;

using PhysicalPlanNode = std::variant<SeqScan, PhysicalProjection, PhysicalFilter, NestedLoopCrossJoin, NestedLoopJoin>;

struct SeqScan {
  std::string table;

  bool operator==(const SeqScan&) const = default;
};

struct PhysicalProjection {
  std::shared_ptr<PhysicalPlanNode> source;
  std::vector<Expression> expressions;

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

} // namespace stewkk::sql
