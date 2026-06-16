#include <stewkk/sql/logic/transformation_rules/aggregation_pushdown_through_join.hpp>

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

bool IsSupportedAggregate(const Expression& expr) {
  return std::holds_alternative<AggregateExpression>(expr);
}

bool AggregatesUseOnlyLeft(const logical::Aggregation& agg,
                           const std::unordered_set<std::string>& lhs_tables) {
  return std::ranges::all_of(agg.aggregates, [&](const Expression& expr) {
    if (!IsSupportedAggregate(expr)) return false;
    const auto& a = std::get<AggregateExpression>(expr);
    return a.is_star || (a.argument && ExprUsesOnlyTables(*a.argument, lhs_tables));
  });
}

bool GroupByCanBeReconstructedAfterJoin(const logical::Aggregation& agg,
                                        const std::unordered_set<std::string>& lhs_tables,
                                        const std::unordered_set<std::string>& rhs_tables) {
  return std::ranges::all_of(agg.group_by, [&](const Expression& expr) {
    return ExprUsesOnlyTables(expr, lhs_tables) || ExprUsesOnlyTables(expr, rhs_tables);
  });
}

bool ContainsExpr(const std::vector<Expression>& exprs, const Expression& needle) {
  return std::ranges::any_of(exprs, [&](const Expression& e) { return e == needle; });
}

std::vector<Expression> PartialGroupBy(const logical::Aggregation& agg,
                                       const std::unordered_set<std::string>& lhs_tables,
                                       const Attribute& join_key) {
  std::vector<Expression> out;
  for (const auto& expr : agg.group_by) {
    if (ExprUsesOnlyTables(expr, lhs_tables)) {
      out.push_back(expr);
    }
  }
  if (!ContainsExpr(out, Expression{join_key})) {
    out.push_back(join_key);
  }
  return out;
}

std::vector<Expression> FinalAggregates(const std::vector<Expression>& aggregates) {
  std::vector<Expression> out;
  out.reserve(aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    out.push_back(AggregateExpression{
        AggregateFunction::kSum,
        std::make_shared<Expression>(Attribute{"", std::format("__agg{}", i)}),
        false});
  }
  return out;
}

bool CanApply(const logical::Aggregation& agg) {
  const auto* join = FindInnerJoin(agg.source);
  if (join == nullptr || !EquiJoinKeys(*join)) return false;
  auto lhs_tables = GroupTables(join->lhs);
  auto rhs_tables = GroupTables(join->rhs);
  return AggregatesUseOnlyLeft(agg, lhs_tables)
      && GroupByCanBeReconstructedAfterJoin(agg, lhs_tables, rhs_tables);
}

}  // namespace

bool AggregationPushdownThroughJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr,
                                                  RuleContext&) {
  if (!std::holds_alternative<logical::Aggregation>(expr->root_operator)) return false;
  return CanApply(std::get<logical::Aggregation>(expr->root_operator));
}

LogicalOperator AggregationPushdownThroughJoin::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                          Memo& memo, RuleContext&) {
  const auto& agg = std::get<logical::Aggregation>(expr->root_operator);
  const auto* join = FindInnerJoin(agg.source);
  auto keys = join == nullptr ? std::optional<JoinKeys>{} : EquiJoinKeys(*join);
  if (join == nullptr || !keys) {
    throw std::runtime_error{"AggregationPushdownThroughJoin requires an inner equijoin"};
  }

  auto lhs_tables = GroupTables(join->lhs);
  auto partial_group_by = PartialGroupBy(agg, lhs_tables, keys->lhs);

  auto partial = memo.AddGroup(
      logical::PartialAggregation{join->lhs, partial_group_by, agg.aggregates})->group;
  auto new_join = memo.AddGroup(
      logical::Join{partial, join->rhs, join->type, join->qual})->group;
  return logical::FinalAggregation{new_join, agg.group_by, FinalAggregates(agg.aggregates)};
}

}  // namespace stewkk::sql
