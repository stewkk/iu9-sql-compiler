#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>

#include <algorithm>
#include <unordered_set>
#include <vector>

#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

bool JoinAssociativity::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
  const auto& outer = std::get<logical::Join>(expr->root_operator);
  if (outer.type != JoinType::kInner) return false;
  for (auto inner_expr : outer.lhs->GetLogicalExprs()) {
    const auto* inner = std::get_if<logical::Join>(&inner_expr->root_operator);
    if (inner != nullptr && inner->type == JoinType::kInner) return true;
  }
  return false;
}

LogicalOperator JoinAssociativity::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
  const auto& outer = std::get<logical::Join>(expr->root_operator);
  for (auto inner_expr : outer.lhs->GetLogicalExprs()) {
    if (!std::holds_alternative<logical::Join>(inner_expr->root_operator)) continue;
    const auto& inner = std::get<logical::Join>(inner_expr->root_operator);
    if (outer.type != JoinType::kInner || inner.type != JoinType::kInner) continue;

    auto bc_tables = GroupTables(inner.rhs);
    auto c_tables = GroupTables(outer.rhs);
    bc_tables.insert(c_tables.begin(), c_tables.end());

    std::vector<Expression> conjs;
    CollectConjuncts(inner.qual, conjs);
    CollectConjuncts(outer.qual, conjs);

    std::vector<Expression> inner_quals;
    std::vector<Expression> outer_quals;
    for (auto& q : conjs) {
      auto q_tables = ExprTables(q);
      bool fits_inner = std::all_of(q_tables.begin(), q_tables.end(),
                                    [&](const auto& t) { return bc_tables.contains(t); });
      (fits_inner ? inner_quals : outer_quals).push_back(std::move(q));
    }

    auto new_rhs = memo.AddGroup(logical::Join{
        inner.rhs, outer.rhs, outer.type, AndConjuncts(inner_quals)})->group;
    // FIXME: should return more than one operators!!!
    return logical::Join{inner.lhs, new_rhs, inner.type, AndConjuncts(outer_quals)};
  }
  throw std::runtime_error{"cant perform JoinAssociativity"};
}

}  // namespace stewkk::sql
