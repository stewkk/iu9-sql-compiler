#include <stewkk/sql/logic/implementation_rules/implement_aggregation.hpp>

namespace stewkk::sql {

bool ImplementAggregation::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::Aggregation>(expr->root_operator);
}

utils::NotNull<PhysicalExpr*> ImplementAggregation::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& agg = std::get<logical::Aggregation>(expr->root_operator);
    return expr->group->AddPhysicalExpr(
        physical::Aggregation{agg.source, agg.group_by, agg.aggregates});
}

}  // namespace stewkk::sql
