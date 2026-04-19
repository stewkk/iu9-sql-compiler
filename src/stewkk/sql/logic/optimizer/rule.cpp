#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

utils::NotNull<LogicalExpr*> TransformationRule::Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
return expr->group->AddLogicalExpr(ApplyImpl(expr, memo));
}

}  // namespace stewkk::sql
