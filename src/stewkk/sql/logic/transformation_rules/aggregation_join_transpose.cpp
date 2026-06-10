#include <stewkk/sql/logic/transformation_rules/aggregation_join_transpose.hpp>

#include <algorithm>
#include <format>
#include <memory>
#include <optional>
#include <stdexcept>

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

struct JoinKeys {
  Attribute lhs;
  Attribute rhs;
};

const logical::Join* FindInnerJoin(utils::NotNull<Group*> source) {
  for (auto inner_expr : source->GetLogicalExprs()) {
    if (const auto* j = std::get_if<logical::Join>(&inner_expr->root_operator);
        j != nullptr && j->type == JoinType::kInner) {
      return j;
    }
  }
  return nullptr;
}

std::optional<JoinKeys> EquiJoinKeys(const logical::Join& join) {
  const auto* bin = std::get_if<BinaryExpression>(&join.qual);
  if (bin == nullptr || bin->binop != BinaryOp::kEq) return std::nullopt;
  const auto* l = std::get_if<Attribute>(bin->lhs.get());
  const auto* r = std::get_if<Attribute>(bin->rhs.get());
  if (l == nullptr || r == nullptr) return std::nullopt;
  auto lhs_tables = GroupTables(join.lhs);
  auto rhs_tables = GroupTables(join.rhs);
  if (lhs_tables.contains(l->table) && rhs_tables.contains(r->table)) {
    return JoinKeys{*l, *r};
  }
  if (lhs_tables.contains(r->table) && rhs_tables.contains(l->table)) {
    return JoinKeys{*r, *l};
  }
  return std::nullopt;
}

bool ContainsExpr(const std::vector<Expression>& exprs, const Expression& needle) {
  return std::ranges::any_of(exprs, [&](const Expression& e) { return e == needle; });
}

bool AggregateInputsOnLeft(const logical::Aggregation& agg,
                           const std::unordered_set<std::string>& lhs_tables) {
  return std::ranges::all_of(agg.aggregates, [&](const Expression& expr) {
    const auto* a = std::get_if<AggregateExpression>(&expr);
    return a != nullptr
        && (a->is_star || (a->argument && ExprUsesOnlyTables(*a->argument, lhs_tables)));
  });
}

bool GroupByOnLeft(const logical::Aggregation& agg,
                   const std::unordered_set<std::string>& lhs_tables) {
  return std::ranges::all_of(agg.group_by, [&](const Expression& expr) {
    return ExprUsesOnlyTables(expr, lhs_tables);
  });
}

std::vector<Expression> OutputProjection(const logical::Aggregation& agg) {
  std::vector<Expression> out = agg.group_by;
  for (size_t i = 0; i < agg.aggregates.size(); ++i) {
    out.push_back(Attribute{"", std::format("__agg{}", i)});
  }
  return out;
}

bool CanApply(const logical::Aggregation& agg, RuleContext& ctx) {
  const auto* join = FindInnerJoin(agg.source);
  if (join == nullptr) return false;
  auto keys = EquiJoinKeys(*join);
  if (!keys) return false;
  auto lhs_tables = GroupTables(join->lhs);
  if (!AggregateInputsOnLeft(agg, lhs_tables) || !GroupByOnLeft(agg, lhs_tables)) {
    return false;
  }
  if (!ContainsExpr(agg.group_by, Expression{keys->lhs})) {
    return false;
  }
  return ctx.constraints.IsUnique(keys->rhs)
      && ctx.constraints.HasForeignKey(keys->lhs, keys->rhs);
}

}  // namespace

bool AggregationJoinTranspose::IsApplicable(utils::NotNull<LogicalExpr*> expr,
                                            RuleContext& ctx) {
  if (!std::holds_alternative<logical::Aggregation>(expr->root_operator)) return false;
  return CanApply(std::get<logical::Aggregation>(expr->root_operator), ctx);
}

LogicalOperator AggregationJoinTranspose::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                    Memo& memo, RuleContext& ctx) {
  const auto& agg = std::get<logical::Aggregation>(expr->root_operator);
  if (!CanApply(agg, ctx)) {
    throw std::runtime_error{"AggregationJoinTranspose requires unique/full inner equijoin"};
  }
  const auto* join = FindInnerJoin(agg.source);
  auto agg_lhs = memo.AddGroup(
      logical::Aggregation{join->lhs, agg.group_by, agg.aggregates})->group;
  auto new_join = memo.AddGroup(
      logical::Join{agg_lhs, join->rhs, join->type, join->qual})->group;
  return logical::Projection{new_join, OutputProjection(agg), {}};
}

}  // namespace stewkk::sql
