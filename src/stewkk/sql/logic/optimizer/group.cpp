#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

utils::NotNull<LogicalExpr*> Group::AddLogicalExpr(LogicalOperator root_operator) {
    auto& ptr = logical_exprs_.emplace_back(
        std::make_unique<LogicalExpr>(std::move(root_operator), this));
    return ptr.get();
}

utils::NotNull<PhysicalExpr*> Group::AddPhysicalExpr() {
    auto& ptr = physical_exprs_.emplace_back(
        std::make_unique<PhysicalExpr>(this));
    return ptr.get();
}

std::span<const Group::LogicalExprPtr> Group::GetLogicalExprs() const {
    return logical_exprs_;
}

}  // namespace stewkk::sql
