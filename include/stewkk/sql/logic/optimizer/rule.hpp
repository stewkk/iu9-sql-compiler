#pragma once

#include <stewkk/sql/logic/optimizer/logical_expr.hpp>
#include <stewkk/sql/logic/optimizer/physical_expr.hpp>

namespace stewkk::sql {

class Memo;

class TransformationRule {
  public:
    virtual bool IsApplicable(utils::NotNull<LogicalExpr*> expr) = 0;
    virtual utils::NotNull<LogicalExpr*> Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) = 0;
    virtual ~TransformationRule() = default;
};

class ImplementationRule {
  public:
    virtual bool IsApplicable(utils::NotNull<LogicalExpr*> expr) = 0;
    virtual utils::NotNull<PhysicalExpr*> Apply(utils::NotNull<LogicalExpr*>, Memo& memo) = 0;
    virtual ~ImplementationRule() = default;
};

// FIXME: enforcer rules?

}  // namespace stewkk::sql
