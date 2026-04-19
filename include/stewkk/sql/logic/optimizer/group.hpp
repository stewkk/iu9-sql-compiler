#pragma once

#include <vector>
#include <memory>
#include <span>

#include <stewkk/sql/logic/optimizer/logical_expr.hpp>
#include <stewkk/sql/logic/optimizer/physical_expr.hpp>

namespace stewkk::sql {

using LogicalOperator = decltype(LogicalExpr::root_operator);
using RuleId = size_t;

class Group {
  public:
    using LogicalExprPtr  = std::unique_ptr<LogicalExpr>;
    using PhysicalExprPtr = std::unique_ptr<PhysicalExpr>;

    utils::NotNull<LogicalExpr*> AddLogicalExpr(LogicalOperator root_operator);
    utils::NotNull<PhysicalExpr*> AddPhysicalExpr();
    std::span<const LogicalExprPtr> GetLogicalExprs() const;

  private:
    friend class Memo;
    Group() = default;

    std::vector<LogicalExprPtr>  logical_exprs_;
    std::vector<PhysicalExprPtr> physical_exprs_;
};

}  // namespace stewkk::sql
