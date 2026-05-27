#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

// Filter(p, Join(L, R, T, q))  →  [Filter(rest,)] Join(maybeFilter(p_L,L), maybeFilter(p_R,R), T, q)
// Conjuncts of p whose attributes lie entirely within one side are pushed
// into that side; conjuncts spanning both sides remain above the join.
// Outer-join safety: a conjunct may only be pushed into a *preserved* side —
// LEFT preserves lhs, RIGHT preserves rhs, INNER preserves both, FULL neither.
class FilterPushdownThroughJoin : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
