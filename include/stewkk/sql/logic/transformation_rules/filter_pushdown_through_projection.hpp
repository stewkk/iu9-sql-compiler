#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

// Filter(p, Projection(exprs, X))  →  Projection(exprs, Filter(p, X))
// Safe only when every Attribute referenced by p appears as a bare Attribute
// in exprs (i.e. is passed through unchanged). Computed projections cannot be
// referenced by name in this AST, so the conservative bare-attribute check
// also matches the wider correctness condition.
class FilterPushdownThroughProjection : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
