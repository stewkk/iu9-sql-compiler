#pragma once

#include <vector>
#include <memory>
#include <span>

#include <stewkk/sql/logic/optimizer/logical_expr.hpp>
#include <stewkk/sql/logic/optimizer/physical_expr.hpp>

namespace stewkk::sql {

using LogicalOperator = decltype(LogicalExpr::root_operator);

class Group {
  public:
    using LogicalExprPtr  = std::unique_ptr<LogicalExpr>;
    using PhysicalExprPtr = std::unique_ptr<PhysicalExpr>;

    utils::NotNull<LogicalExpr*> AddLogicalExpr(LogicalOperator root_operator) {
        auto& ptr = logical_exprs_.emplace_back(
            std::make_unique<LogicalExpr>(std::move(root_operator), this));
        return ptr.get();
    }

    utils::NotNull<PhysicalExpr*> AddPhysicalExpr() {
        auto& ptr = physical_exprs_.emplace_back(
            std::make_unique<PhysicalExpr>(this));
        return ptr.get();
    }

    std::span<const LogicalExprPtr> GetLogicalExprs() const { return logical_exprs_; }

  private:
    friend class Memo;
    Group() = default;

    std::vector<LogicalExprPtr>  logical_exprs_;
    std::vector<PhysicalExprPtr> physical_exprs_;
};

}  // namespace stewkk::sql
