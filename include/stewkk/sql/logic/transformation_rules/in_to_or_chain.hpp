#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

// Filter(... x IN (v0, v1, ...) ..., X)
//   →  Filter(... (x = v0 OR x = v1 OR ...) ..., X)
// and the NOT IN case to (x != v0 AND x != v1 AND ...).
//
// IN and the OR-chain are equal under three-valued logic, so this just adds the
// expanded form as a sibling in the same group. It lets the exhaustive search
// reach plans that materialize the OR-chain (as MS SQL Server does), and the
// expanded predicate is JIT-able whereas the InExpression node is not.
class InToOrChain : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
