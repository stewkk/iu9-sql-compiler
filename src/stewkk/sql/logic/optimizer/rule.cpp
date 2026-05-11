#include <stewkk/sql/logic/optimizer/rule.hpp>

#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

utils::NotNull<LogicalExpr*> TransformationRule::Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
    // FIXME: maybe it should be Memo's responsibility to check for duplicates?
    auto op = ApplyImpl(expr, memo);
    auto existing = memo.GetGroup(op);
    if (existing) {
        return existing;
    }
    return expr->group->AddLogicalExpr(std::move(op));
}

}  // namespace stewkk::sql
