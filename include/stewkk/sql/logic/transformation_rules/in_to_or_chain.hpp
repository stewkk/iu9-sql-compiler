#pragma once

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

class InToOrChain : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    LogicalOperator ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
