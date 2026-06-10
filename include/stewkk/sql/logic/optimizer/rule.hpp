#pragma once

#include <cstddef>
#include <vector>

#include <stewkk/sql/logic/optimizer/logical_expr.hpp>
#include <stewkk/sql/logic/optimizer/physical_expr.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>

namespace stewkk::sql {

class Memo;

struct RuleContext {
    SchemaCatalog& schema;
    ConstraintCatalog& constraints;
};

class TransformationRule {
  public:
    virtual bool IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext& ctx) = 0;
    utils::NotNull<LogicalExpr*> Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo, RuleContext& ctx);
    virtual ~TransformationRule() = default;

  private:
    virtual LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo, RuleContext& ctx) = 0;
};

class ImplementationRule {
  public:
    virtual bool IsApplicable(utils::NotNull<LogicalExpr*> expr) = 0;
    virtual std::vector<utils::NotNull<PhysicalExpr*>> Apply(utils::NotNull<LogicalExpr*>, Memo& memo) = 0;
    virtual ~ImplementationRule() = default;
};

}  // namespace stewkk::sql
