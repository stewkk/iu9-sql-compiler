#include <stewkk/sql/logic/transformation_rules/filter_to_join_predicate.hpp>

#include <algorithm>
#include <stdexcept>
#include <vector>

#include <stewkk/sql/logic/optimizer/memo.hpp>
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

bool UsesBothJoinSides(const Expression& expr, const logical::Join& join) {
  const auto tables = ExprTables(expr);
  const auto lhs_tables = GroupTables(join.lhs);
  const auto rhs_tables = GroupTables(join.rhs);

  bool uses_lhs = false;
  bool uses_rhs = false;
  for (const auto& table : tables) {
    uses_lhs = uses_lhs || lhs_tables.contains(table);
    uses_rhs = uses_rhs || rhs_tables.contains(table);
  }
  return uses_lhs && uses_rhs;
}

bool HasCrossSideConjunct(const Expression& predicate, const logical::Join& join) {
  std::vector<Expression> conjs;
  CollectConjuncts(predicate, conjs);
  return std::ranges::any_of(conjs, [&](const Expression& conj) {
    return UsesBothJoinSides(conj, join);
  });
}

}  // namespace

bool FilterToJoinPredicate::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  const auto* join = FindInnerJoin(filter.source);
  return join != nullptr && HasCrossSideConjunct(filter.predicate, *join);
}

LogicalOperator FilterToJoinPredicate::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                 Memo& memo, RuleContext&) {
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  const auto* join = FindInnerJoin(filter.source);
  if (join == nullptr) {
    throw std::runtime_error{"FilterToJoinPredicate requires an inner join below filter"};
  }

  std::vector<Expression> join_conjs;
  std::vector<Expression> rest_conjs;
  CollectConjuncts(join->qual, join_conjs);

  std::vector<Expression> filter_conjs;
  CollectConjuncts(filter.predicate, filter_conjs);
  for (const auto& conj : filter_conjs) {
    if (UsesBothJoinSides(conj, *join)) {
      join_conjs.push_back(conj);
    } else {
      rest_conjs.push_back(conj);
    }
  }

  auto new_join = logical::Join{join->lhs, join->rhs, join->type, AndConjuncts(join_conjs)};
  if (rest_conjs.empty()) {
    return new_join;
  }
  auto new_join_group = memo.AddGroup(new_join)->group;
  return logical::Filter{new_join_group, AndConjuncts(rest_conjs)};
}

}  // namespace stewkk::sql
