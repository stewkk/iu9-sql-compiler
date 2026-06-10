#include <stewkk/sql/logic/transformation_rules/filter_to_join_predicate.hpp>

#include <stdexcept>
#include <vector>

#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

const logical::Join* FindInnerJoin(utils::NotNull<Group*> source) {
  for (auto inner_expr : source->GetLogicalExprs()) {
    if (const auto* j = std::get_if<logical::Join>(&inner_expr->root_operator);
        j != nullptr && j->type == JoinType::kInner) {
      return j;
    }
  }
  return nullptr;
}

}  // namespace

bool FilterToJoinPredicate::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  return FindInnerJoin(filter.source) != nullptr;
}

LogicalOperator FilterToJoinPredicate::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                 Memo& memo, RuleContext&) {
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  const auto* join = FindInnerJoin(filter.source);
  if (join == nullptr) {
    throw std::runtime_error{"FilterToJoinPredicate requires an inner join below filter"};
  }

  std::vector<Expression> conjs;
  CollectConjuncts(join->qual, conjs);
  CollectConjuncts(filter.predicate, conjs);
  return logical::Join{join->lhs, join->rhs, join->type, AndConjuncts(conjs)};
}

}  // namespace stewkk::sql
