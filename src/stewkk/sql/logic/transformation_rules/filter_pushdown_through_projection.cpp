#include <stewkk/sql/logic/transformation_rules/filter_pushdown_through_projection.hpp>

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>
#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

void CollectAttributes(const Expression& e, std::vector<Attribute>& out) {
  std::visit(utils::Overloaded{
      [&](const Attribute& a) { out.push_back(a); },
      [&](const BinaryExpression& b) {
        CollectAttributes(*b.lhs, out);
        CollectAttributes(*b.rhs, out);
      },
      [&](const UnaryExpression& u) { CollectAttributes(*u.child, out); },
      [&](const InExpression& i) {
        CollectAttributes(*i.lhs, out);
        for (const auto& value : i.values) {
          CollectAttributes(value, out);
        }
      },
      [&](const AggregateExpression& a) {
        if (!a.is_star && a.argument) {
          CollectAttributes(*a.argument, out);
        }
      },
      [&](const IntConst&) {},
      [&](const StringConst&) {},
      [&](const Literal&) {},
  }, e);
}

bool ProjectionPassesThrough(const std::vector<Expression>& proj_exprs,
                             const std::vector<Attribute>& needed) {
  return std::all_of(needed.begin(), needed.end(), [&](const Attribute& a) {
    return std::any_of(proj_exprs.begin(), proj_exprs.end(), [&](const Expression& e) {
      const auto* pa = std::get_if<Attribute>(&e);
      return pa != nullptr && *pa == a;
    });
  });
}

const logical::Projection* FindPushableProjection(utils::NotNull<Group*> source,
                                                  const Expression& predicate) {
  std::vector<Attribute> needed;
  CollectAttributes(predicate, needed);
  for (auto inner_expr : source->GetLogicalExprs()) {
    if (const auto* p = std::get_if<logical::Projection>(&inner_expr->root_operator)) {
      if (ProjectionPassesThrough(p->expressions, needed)) {
        return p;
      }
    }
  }
  return nullptr;
}

}  // namespace

bool FilterPushdownThroughProjection::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  return FindPushableProjection(f.source, f.predicate) != nullptr;
}

LogicalOperator FilterPushdownThroughProjection::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                           Memo& memo) {
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  const auto* proj = FindPushableProjection(f.source, f.predicate);
  if (proj == nullptr) {
    throw std::runtime_error{"FilterPushdownThroughProjection requires a passthrough projection"};
  }

  auto new_filter_group = memo.AddGroup(logical::Filter{proj->source, f.predicate})->group;
  return logical::Projection{new_filter_group, proj->expressions, proj->aliases};
}

}  // namespace stewkk::sql
