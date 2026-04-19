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

    size_t id() const;

  private:
    friend class Memo;
    explicit Group(size_t id) : id_(id) {}

    size_t id_;
    std::vector<LogicalExprPtr>  logical_exprs_;
    std::vector<PhysicalExprPtr> physical_exprs_;
};

}  // namespace stewkk::sql
