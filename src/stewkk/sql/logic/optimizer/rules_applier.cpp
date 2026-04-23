#include <stewkk/sql/logic/optimizer/rules_applier.hpp>

namespace stewkk::sql {

template<size_t NTransformation, size_t NImplementation>
RulesApplier<NTransformation, NImplementation>::RulesApplier(Rules<NTransformation, NImplementation> rules)
    : rules_(std::move(rules)) {}

template<size_t NTransformation, size_t NImplementation>
bool RulesApplier<NTransformation, NImplementation>::IsApplicable(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr) {
    return !applied_transformation_rules_[expr.get()][rule.value] &&
           rules_.transformation_rules[rule.value]->IsApplicable(expr);
}

template<size_t NTransformation, size_t NImplementation>
utils::NotNull<LogicalExpr*> RulesApplier<NTransformation, NImplementation>::Apply(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Memo& memo) {
    applied_transformation_rules_[expr.get()][rule.value] = 1;
    return rules_.transformation_rules[rule.value]->Apply(expr, memo);
}

template<size_t NTransformation, size_t NImplementation>
bool RulesApplier<NTransformation, NImplementation>::IsApplicable(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr) {
    return rules_.implementation_rules[rule.value]->IsApplicable(expr);
}

template<size_t NTransformation, size_t NImplementation>
utils::NotNull<PhysicalExpr*> RulesApplier<NTransformation, NImplementation>::Apply(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr, Memo& memo) {
    return rules_.implementation_rules[rule.value]->Apply(expr, memo);
}

template class RulesApplier<2, 0>;
template class RulesApplier<2, 5>;

}  // namespace stewkk::sql
