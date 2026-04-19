#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>

#include <memory>

namespace stewkk::sql {

bool JoinAssociativity::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
  const auto& outer = std::get<logical::Join>(expr->root_operator);
  for (const auto& inner_expr : outer.lhs->GetLogicalExprs()) {
    if (std::holds_alternative<logical::Join>(inner_expr->root_operator)) return true;
  }
  return false;
}

// (A ⋈₁ B) ⋈₂ C  →  A ⋈₁∧₂ (B ⋈₂ C)
LogicalOperator JoinAssociativity::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
  const auto& outer = std::get<logical::Join>(expr->root_operator);
  for (const auto& inner_expr : outer.lhs->GetLogicalExprs()) {
    if (!std::holds_alternative<logical::Join>(inner_expr->root_operator)) continue;
    const auto& inner = std::get<logical::Join>(inner_expr->root_operator);
    auto combined_qual = Expression{BinaryExpression{
        std::make_shared<Expression>(inner.qual),
        BinaryOp::kAnd,
        std::make_shared<Expression>(outer.qual),
    }};
    auto new_rhs = memo.AddGroup(logical::Join{inner.rhs, outer.rhs, outer.type, Literal::kTrue});
    // FIXME: should return more than one operators!!!
    return logical::Join{inner.lhs, new_rhs, inner.type, combined_qual};
  }
  throw std::runtime_error{"cant perform JoinAssociativity"};
}

}  // namespace stewkk::sql
