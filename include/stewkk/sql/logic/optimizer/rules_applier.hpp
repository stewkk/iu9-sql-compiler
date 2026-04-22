#pragma once

#include <array>
#include <unordered_map>

#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {


struct TransformationRuleId {
    size_t value;
};

struct ImplementationRuleId {
    size_t value;
};

template<size_t NTransformation, size_t NImplementation>
class RulesApplier {
  public:
    explicit RulesApplier(Rules<NTransformation, NImplementation> rules);

    bool IsApplicable(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr);
    utils::NotNull<LogicalExpr*> Apply(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Memo& memo);

    bool IsApplicable(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr);
    utils::NotNull<PhysicalExpr*> Apply(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr, Memo& memo);

  private:
    Rules<NTransformation, NImplementation> rules_;
    std::unordered_map<LogicalExpr*, std::array<char, NTransformation>> applied_transformation_rules_;
};

} // namespace stewkk::sql
