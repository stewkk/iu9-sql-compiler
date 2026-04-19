#pragma once

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

class JoinCommutativity : public TransformationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    utils::NotNull<LogicalExpr*> Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
