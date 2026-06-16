#include <stewkk/sql/logic/transformation_rules/projection_pushdown_through_join.hpp>

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

const logical::Join* FindJoin(utils::NotNull<Group*> source) {
  for (auto inner_expr : source->GetLogicalExprs()) {
    if (const auto* j = std::get_if<logical::Join>(&inner_expr->root_operator)) {
      return j;
    }
  }
  return nullptr;
}

bool ContainsAttr(const std::vector<Attribute>& attrs, const Attribute& attr) {
  return std::ranges::any_of(attrs, [&](const Attribute& a) { return a == attr; });
}

std::vector<Attribute> NeededForSide(const std::vector<Attribute>& needed,
                                     const Schema& schema) {
  std::vector<Attribute> out;
  for (const auto& attr : needed) {
    auto it = std::ranges::find_if(schema, [&](const Attribute& schema_attr) {
      return schema_attr.name == attr.name
          && (attr.table.empty() || attr.table == schema_attr.table);
    });
    if (it != schema.end() && !ContainsAttr(out, *it)) {
      out.push_back(*it);
    }
  }
  return out;
}

std::vector<Expression> AttrExpressions(const std::vector<Attribute>& attrs) {
  std::vector<Expression> out;
  out.reserve(attrs.size());
  for (const auto& attr : attrs) {
    out.push_back(attr);
  }
  return out;
}

bool CanPush(const logical::Projection& projection, const logical::Join& join,
             RuleContext& ctx) {
  std::vector<Attribute> needed;
  for (const auto& expr : projection.expressions) {
    CollectAttributes(expr, needed);
  }
  CollectAttributes(join.qual, needed);

  auto lhs_schema = ctx.schema.GetSchema(join.lhs);
  auto rhs_schema = ctx.schema.GetSchema(join.rhs);
  auto lhs_needed = NeededForSide(needed, lhs_schema);
  auto rhs_needed = NeededForSide(needed, rhs_schema);
  return (!lhs_needed.empty() && lhs_needed.size() < lhs_schema.size())
      || (!rhs_needed.empty() && rhs_needed.size() < rhs_schema.size());
}

}  // namespace

bool ProjectionPushdownThroughJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr,
                                                 RuleContext& ctx) {
  if (!std::holds_alternative<logical::Projection>(expr->root_operator)) return false;
  const auto& projection = std::get<logical::Projection>(expr->root_operator);
  const auto* join = FindJoin(projection.source);
  return join != nullptr && CanPush(projection, *join, ctx);
}

LogicalOperator ProjectionPushdownThroughJoin::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                         Memo& memo, RuleContext& ctx) {
  const auto& projection = std::get<logical::Projection>(expr->root_operator);
  const auto* join = FindJoin(projection.source);
  if (join == nullptr) {
    throw std::runtime_error{"ProjectionPushdownThroughJoin requires a join below"};
  }

  std::vector<Attribute> needed;
  for (const auto& e : projection.expressions) {
    CollectAttributes(e, needed);
  }
  CollectAttributes(join->qual, needed);

  auto lhs_schema = ctx.schema.GetSchema(join->lhs);
  auto rhs_schema = ctx.schema.GetSchema(join->rhs);
  auto lhs_needed = NeededForSide(needed, lhs_schema);
  auto rhs_needed = NeededForSide(needed, rhs_schema);

  auto lhs = join->lhs;
  if (!lhs_needed.empty() && lhs_needed.size() < lhs_schema.size()) {
    lhs = memo.AddGroup(logical::Projection{join->lhs, AttrExpressions(lhs_needed), {}})->group;
  }
  auto rhs = join->rhs;
  if (!rhs_needed.empty() && rhs_needed.size() < rhs_schema.size()) {
    rhs = memo.AddGroup(logical::Projection{join->rhs, AttrExpressions(rhs_needed), {}})->group;
  }

  auto new_join = memo.AddGroup(logical::Join{lhs, rhs, join->type, join->qual})->group;
  return logical::Projection{new_join, projection.expressions, projection.aliases};
}

}  // namespace stewkk::sql
