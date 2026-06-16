#include <stewkk/sql/logic/transformation_rules/filter_split.hpp>

#include <stdexcept>
#include <vector>

#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

size_t ConjunctCount(const Expression& e) {
  std::vector<Expression> conjs;
  CollectConjuncts(e, conjs);
  return conjs.size();
}

}  // namespace

bool FilterSplit::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  return ConjunctCount(f.predicate) >= 2;
}

LogicalOperator FilterSplit::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo, RuleContext&) {
  const auto& f = std::get<logical::Filter>(expr->root_operator);

  std::vector<Expression> conjs;
  CollectConjuncts(f.predicate, conjs);
  if (conjs.size() < 2) {
    throw std::runtime_error{"FilterSplit requires a conjunction"};
  }

  Expression head = conjs.front();
  std::vector<Expression> rest(conjs.begin() + 1, conjs.end());
  Expression rest_pred = AndConjuncts(rest);

  auto inner_group = memo.AddGroup(logical::Filter{f.source, std::move(rest_pred)})->group;
  return logical::Filter{inner_group, std::move(head)};
}

}  // namespace stewkk::sql
