#pragma once

#include <array>
#include <unordered_map>

#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

template<size_t NTransformation, size_t NImplementation>
class RulesApplier {
  public:
    explicit RulesApplier(Rules<NTransformation, NImplementation> rules)
        : rules_(std::move(rules)) {}

    bool IsApplicable(RuleId rule, utils::NotNull<LogicalExpr*> expr) {
        return !applied_transformation_rules_[expr.get()][rule] &&
               rules_.transformation_rules[rule]->IsApplicable(expr);
    }

    utils::NotNull<LogicalExpr*> Apply(RuleId rule, utils::NotNull<LogicalExpr*> expr, Memo& memo) {
        applied_transformation_rules_[expr.get()][rule] = 1;
        return rules_.transformation_rules[rule]->Apply(expr, memo);
    }

  private:
    Rules<NTransformation, NImplementation> rules_;
    std::unordered_map<LogicalExpr*, std::array<char, NTransformation>> applied_transformation_rules_;
};

} // namespace stewkk::sql
