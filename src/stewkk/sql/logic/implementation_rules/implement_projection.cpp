#include <stewkk/sql/logic/implementation_rules/implement_projection.hpp>

namespace stewkk::sql {

bool ImplementProjection::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::Projection>(expr->root_operator);
}

utils::NotNull<PhysicalExpr*> ImplementProjection::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& proj = std::get<logical::Projection>(expr->root_operator);
    return expr->group->AddPhysicalExpr(physical::Projection{proj.source, proj.expressions});
}

}  // namespace stewkk::sql
