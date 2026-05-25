#include <stewkk/sql/logic/optimizer/rule.hpp>

#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

utils::NotNull<LogicalExpr*> TransformationRule::Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
    return memo.AddLogicalExprToGroup(expr->group, ApplyImpl(expr, memo));
}

}  // namespace stewkk::sql
