#include <stewkk/sql/logic/transformation_rules/cross_join_to_join.hpp>

#include <stdexcept>

namespace stewkk::sql {

namespace {

const logical::CrossJoin* FindCrossJoin(utils::NotNull<Group*> source) {
  for (auto inner_expr : source->GetLogicalExprs()) {
    if (const auto* j = std::get_if<logical::CrossJoin>(&inner_expr->root_operator)) {
      return j;
    }
  }
  return nullptr;
}

}  // namespace

bool CrossJoinToJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  return FindCrossJoin(filter.source) != nullptr;
}

LogicalOperator CrossJoinToJoin::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                           Memo&, RuleContext&) {
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  const auto* cross = FindCrossJoin(filter.source);
  if (cross == nullptr) {
    throw std::runtime_error{"CrossJoinToJoin requires a cross join below filter"};
  }
  return logical::Join{cross->lhs, cross->rhs, JoinType::kInner, filter.predicate};
}

}  // namespace stewkk::sql
