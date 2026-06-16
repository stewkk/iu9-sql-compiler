#include <stewkk/sql/logic/transformation_rules/outer_join_to_inner.hpp>

#include <stdexcept>

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

const logical::Join* FindReducibleJoin(utils::NotNull<Group*> source,
                                       const Expression& predicate) {
  for (auto inner_expr : source->GetLogicalExprs()) {
    const auto* join = std::get_if<logical::Join>(&inner_expr->root_operator);
    if (join == nullptr || join->type == JoinType::kInner) continue;
    auto lhs_tables = GroupTables(join->lhs);
    auto rhs_tables = GroupTables(join->rhs);
    if (join->type == JoinType::kLeft
        && IsNullRejectingForTables(predicate, rhs_tables)) {
      return join;
    }
    if (join->type == JoinType::kRight
        && IsNullRejectingForTables(predicate, lhs_tables)) {
      return join;
    }
    if (join->type == JoinType::kFull
        && IsNullRejectingForTables(predicate, lhs_tables)
        && IsNullRejectingForTables(predicate, rhs_tables)) {
      return join;
    }
  }
  return nullptr;
}

}  // namespace

bool OuterJoinToInner::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  return FindReducibleJoin(filter.source, filter.predicate) != nullptr;
}

LogicalOperator OuterJoinToInner::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                            Memo& memo, RuleContext&) {
  const auto& filter = std::get<logical::Filter>(expr->root_operator);
  const auto* join = FindReducibleJoin(filter.source, filter.predicate);
  if (join == nullptr) {
    throw std::runtime_error{"OuterJoinToInner requires a null-rejecting filter"};
  }
  auto inner = memo.AddGroup(
      logical::Join{join->lhs, join->rhs, JoinType::kInner, join->qual})->group;
  return logical::Filter{inner, filter.predicate};
}

}  // namespace stewkk::sql
