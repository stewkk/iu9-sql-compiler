#include <stewkk/sql/logic/implementation_rules/implement_filter.hpp>

namespace stewkk::sql {

bool ImplementFilter::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::Filter>(expr->root_operator);
}

utils::NotNull<PhysicalExpr*> ImplementFilter::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& filter = std::get<logical::Filter>(expr->root_operator);
    return expr->group->AddPhysicalExpr(physical::Filter{filter.source, filter.predicate});
}

}  // namespace stewkk::sql
