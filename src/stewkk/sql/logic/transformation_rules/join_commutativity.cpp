#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>

namespace stewkk::sql {

bool JoinCommutativity::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  return std::holds_alternative<logical::Join>(expr->root_operator);
}

LogicalOperator JoinCommutativity::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo&) {
  auto join = std::get<logical::Join>(expr->root_operator);
  std::swap(join.lhs, join.rhs);
  return join;
}

}  // namespace stewkk::sql
