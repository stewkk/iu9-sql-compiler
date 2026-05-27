#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

// Filter(p1 AND p2 AND ... AND pn, X)  →  Filter(p1, Filter(p2 AND ... AND pn, X))
// Splits the first conjunct off; remaining conjuncts cascade-split when the
// rule fires on the inner Filter.
class FilterSplit : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
