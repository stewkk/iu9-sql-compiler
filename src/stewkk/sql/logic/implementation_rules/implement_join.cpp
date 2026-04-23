#include <stewkk/sql/logic/implementation_rules/implement_join.hpp>

namespace stewkk::sql {

bool ImplementJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::Join>(expr->root_operator);
}

utils::NotNull<PhysicalExpr*> ImplementJoin::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& join = std::get<logical::Join>(expr->root_operator);
    return expr->group->AddPhysicalExpr(
        physical::NestedLoopJoin{join.lhs, join.rhs, join.type, join.qual});
}

}  // namespace stewkk::sql
