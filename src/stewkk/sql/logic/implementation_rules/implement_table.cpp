#include <stewkk/sql/logic/implementation_rules/implement_table.hpp>

namespace stewkk::sql {

bool ImplementTable::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    return std::holds_alternative<logical::Table>(expr->root_operator);
}

utils::NotNull<PhysicalExpr*> ImplementTable::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& table = std::get<logical::Table>(expr->root_operator);
    return expr->group->AddPhysicalExpr(physical::SeqScan{table.name});
}

}  // namespace stewkk::sql
