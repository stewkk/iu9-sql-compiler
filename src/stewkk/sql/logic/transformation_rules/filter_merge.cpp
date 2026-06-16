#include <stewkk/sql/logic/transformation_rules/filter_merge.hpp>

#include <memory>
#include <stdexcept>

#include <stewkk/sql/models/parser/expression.hpp>

namespace stewkk::sql {

namespace {

const logical::Filter* FindInnerFilter(utils::NotNull<Group*> source) {
  for (auto inner_expr : source->GetLogicalExprs()) {
    if (const auto* f = std::get_if<logical::Filter>(&inner_expr->root_operator)) {
      return f;
    }
  }
  return nullptr;
}

}  // namespace

bool FilterMerge::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& outer = std::get<logical::Filter>(expr->root_operator);
  return FindInnerFilter(outer.source) != nullptr;
}

LogicalOperator FilterMerge::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo&, RuleContext&) {
  const auto& outer = std::get<logical::Filter>(expr->root_operator);
  const auto* inner = FindInnerFilter(outer.source);
  if (inner == nullptr) {
    throw std::runtime_error{"FilterMerge requires a nested filter"};
  }

  Expression merged = BinaryExpression{
      std::make_shared<Expression>(outer.predicate),
      BinaryOp::kAnd,
      std::make_shared<Expression>(inner->predicate),
  };
  return logical::Filter{inner->source, std::move(merged)};
}

}  // namespace stewkk::sql
