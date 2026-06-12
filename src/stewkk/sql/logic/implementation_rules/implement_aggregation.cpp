#include <stewkk/sql/logic/implementation_rules/implement_aggregation.hpp>

#include <algorithm>

namespace stewkk::sql {

bool ImplementAggregation::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::Aggregation>(expr->root_operator);
}

std::vector<utils::NotNull<PhysicalExpr*>> ImplementAggregation::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& agg = std::get<logical::Aggregation>(expr->root_operator);
    std::vector<utils::NotNull<PhysicalExpr*>> result{
        expr->group->AddPhysicalExpr(
            physical::Aggregation{agg.source, agg.group_by, agg.aggregates})};
    if (!agg.group_by.empty()
        && std::ranges::all_of(agg.group_by, [](const Expression& group_expr) {
             return std::holds_alternative<Attribute>(group_expr);
           })) {
        result.push_back(expr->group->AddPhysicalExpr(
            physical::StreamAggregation{agg.source, agg.group_by, agg.aggregates}));
    }
    return result;
}

}  // namespace stewkk::sql
