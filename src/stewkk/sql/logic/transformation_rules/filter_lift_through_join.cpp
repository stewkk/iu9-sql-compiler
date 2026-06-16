#include <stewkk/sql/logic/transformation_rules/filter_lift_through_join.hpp>

#include <optional>
#include <stdexcept>

#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

namespace {

struct LiftCandidate {
  logical::Filter filter;
  bool left;
};

std::optional<LiftCandidate> FindCandidate(const logical::Join& join) {
  const bool can_left = join.type == JoinType::kInner || join.type == JoinType::kLeft;
  const bool can_right = join.type == JoinType::kInner || join.type == JoinType::kRight;
  if (can_left) {
    for (auto expr : join.lhs->GetLogicalExprs()) {
      if (const auto* f = std::get_if<logical::Filter>(&expr->root_operator)) {
        return LiftCandidate{*f, true};
      }
    }
  }
  if (can_right) {
    for (auto expr : join.rhs->GetLogicalExprs()) {
      if (const auto* f = std::get_if<logical::Filter>(&expr->root_operator)) {
        return LiftCandidate{*f, false};
      }
    }
  }
  return std::nullopt;
}

}  // namespace

bool FilterLiftThroughJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr, RuleContext&) {
  if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
  return FindCandidate(std::get<logical::Join>(expr->root_operator)).has_value();
}

LogicalOperator FilterLiftThroughJoin::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                 Memo& memo, RuleContext&) {
  const auto& join = std::get<logical::Join>(expr->root_operator);
  auto candidate = FindCandidate(join);
  if (!candidate) {
    throw std::runtime_error{"FilterLiftThroughJoin requires a child filter"};
  }
  auto new_lhs = candidate->left ? candidate->filter.source : join.lhs;
  auto new_rhs = candidate->left ? join.rhs : candidate->filter.source;
  auto new_join = memo.AddGroup(logical::Join{new_lhs, new_rhs, join.type, join.qual})->group;
  return logical::Filter{new_join, candidate->filter.predicate};
}

}  // namespace stewkk::sql
