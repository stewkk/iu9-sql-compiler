#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

class FilterMerge : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext& ctx) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo, RuleContext& ctx) override;
};

}  // namespace stewkk::sql
