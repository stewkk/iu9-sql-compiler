#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

// Filter(p_outer, Filter(p_inner, X))  →  Filter(p_outer AND p_inner, X)
// Collapses adjacent filters so the merged predicate can be pushed down or
// matched against join quals as a single conjunction.
class FilterMerge : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
