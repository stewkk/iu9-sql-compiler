#pragma once

#include <stewkk/sql/logic/optimizer/rule.hpp>

namespace stewkk::sql {

class ImplementHashJoin : public ImplementationRule {
  public:
    bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
    std::vector<utils::NotNull<PhysicalExpr*>> Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;
};

}  // namespace stewkk::sql
