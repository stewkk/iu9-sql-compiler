#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>

namespace stewkk::sql {

bool JoinCommutativity::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  return std::holds_alternative<logical::Join>(expr->root_operator);
}

utils::NotNull<LogicalExpr*> JoinCommutativity::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
  auto join = std::get<logical::Join>(expr->root_operator);
  std::swap(join.lhs, join.rhs);
  return expr->group->AddLogicalExpr(std::move(join));
}

}  // namespace stewkk::sql
