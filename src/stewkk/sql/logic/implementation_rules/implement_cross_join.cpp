#include <stewkk/sql/logic/implementation_rules/implement_cross_join.hpp>

namespace stewkk::sql {

bool ImplementCrossJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::CrossJoin>(expr->root_operator);
}

utils::NotNull<PhysicalExpr*> ImplementCrossJoin::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& cj = std::get<logical::CrossJoin>(expr->root_operator);
    return expr->group->AddPhysicalExpr(physical::NestedLoopCrossJoin{cj.lhs, cj.rhs});
}

}  // namespace stewkk::sql
