#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

utils::NotNull<LogicalExpr*> Group::AddLogicalExpr(LogicalOperator root_operator) {
    return &logical_exprs_.emplace_back(std::move(root_operator), this);
}

utils::NotNull<PhysicalExpr*> Group::AddPhysicalExpr(PhysicalOperator root_operator, bool is_enforcer) {
    return &physical_exprs_.emplace_back(std::move(root_operator), this, is_enforcer);
}

Group::LogicalExprs Group::GetLogicalExprs() {
    return std::views::transform(logical_exprs_, ToNotNull{});
}

Group::PhysicalExprs Group::GetPhysicalExprs() {
    return std::views::transform(physical_exprs_, ToNotNullPhysical{});
}

size_t Group::GetId() const {
    return id_;
}

}  // namespace stewkk::sql
