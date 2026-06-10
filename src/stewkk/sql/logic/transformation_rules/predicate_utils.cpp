#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

#include <algorithm>
#include <memory>
#include <utility>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

bool IsTrue(const Expression& e) {
  return std::holds_alternative<Literal>(e) && std::get<Literal>(e) == Literal::kTrue;
}

void CollectConjuncts(const Expression& e, std::vector<Expression>& out) {
  if (IsTrue(e)) return;
  if (const auto* b = std::get_if<BinaryExpression>(&e); b && b->binop == BinaryOp::kAnd) {
    CollectConjuncts(*b->lhs, out);
    CollectConjuncts(*b->rhs, out);
    return;
  }
  out.push_back(e);
}

Expression AndConjuncts(const std::vector<Expression>& conjs) {
  if (conjs.empty()) return Literal::kTrue;
  Expression acc = conjs[0];
  for (size_t i = 1; i < conjs.size(); i++) {
    acc = BinaryExpression{
        std::make_shared<Expression>(std::move(acc)),
        BinaryOp::kAnd,
        std::make_shared<Expression>(conjs[i]),
    };
  }
  return acc;
}

void CollectAttrTables(const Expression& e, std::unordered_set<std::string>& out) {
  std::visit(utils::Overloaded{
      [&](const Attribute& a) { out.insert(a.table); },
      [&](const BinaryExpression& b) {
        CollectAttrTables(*b.lhs, out);
        CollectAttrTables(*b.rhs, out);
      },
      [&](const UnaryExpression& u) { CollectAttrTables(*u.child, out); },
      [&](const InExpression& i) {
        CollectAttrTables(*i.lhs, out);
        for (const auto& value : i.values) {
          CollectAttrTables(value, out);
        }
      },
      [&](const AggregateExpression& a) {
        if (!a.is_star && a.argument) {
          CollectAttrTables(*a.argument, out);
        }
      },
      [&](const IntConst&) {},
      [&](const StringConst&) {},
      [&](const Literal&) {},
  }, e);
}

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

std::unordered_set<std::string> ExprTables(const Expression& e) {
  std::unordered_set<std::string> out;
  CollectAttrTables(e, out);
  return out;
}

bool ExprUsesOnlyTables(const Expression& e, const std::unordered_set<std::string>& tables) {
  auto expr_tables = ExprTables(e);
  return std::all_of(expr_tables.begin(), expr_tables.end(),
                     [&](const auto& table) { return tables.contains(table); });
}

bool IsNullRejectingForTables(const Expression& e,
                              const std::unordered_set<std::string>& tables) {
  return std::visit(utils::Overloaded{
      [&](const Attribute& a) { return tables.contains(a.table); },
      [&](const BinaryExpression& b) {
        if (b.binop == BinaryOp::kAnd) {
          return IsNullRejectingForTables(*b.lhs, tables)
              || IsNullRejectingForTables(*b.rhs, tables);
        }
        if (b.binop == BinaryOp::kOr) {
          return IsNullRejectingForTables(*b.lhs, tables)
              && IsNullRejectingForTables(*b.rhs, tables);
        }
        if (b.binop == BinaryOp::kEq || b.binop == BinaryOp::kNotEq
            || b.binop == BinaryOp::kGt || b.binop == BinaryOp::kLt
            || b.binop == BinaryOp::kGe || b.binop == BinaryOp::kLe) {
          return IsNullRejectingForTables(*b.lhs, tables)
              || IsNullRejectingForTables(*b.rhs, tables);
        }
        return false;
      },
      [&](const UnaryExpression& u) {
        if (u.op == UnaryOp::kIsNull) return false;
        return IsNullRejectingForTables(*u.child, tables);
      },
      [&](const InExpression& i) {
        return IsNullRejectingForTables(*i.lhs, tables);
      },
      [&](const AggregateExpression& a) {
        return !a.is_star && a.argument && IsNullRejectingForTables(*a.argument, tables);
      },
      [&](const IntConst&) { return false; },
      [&](const StringConst&) { return false; },
      [&](const Literal&) { return false; },
  }, e);
}

namespace {

void CollectGroupTables(utils::NotNull<Group*> g, std::unordered_set<std::string>& out,
                        std::unordered_set<Group*>& seen) {
  if (!seen.insert(g.get()).second) return;
  auto exprs = g->GetLogicalExprs();
  if (exprs.empty()) return;
  std::visit(utils::Overloaded{
      [&](const logical::Table& t) { out.insert(std::string{VisibleName(t)}); },
      [&](const logical::Filter& f) { CollectGroupTables(f.source, out, seen); },
      [&](const logical::Projection& p) { CollectGroupTables(p.source, out, seen); },
      [&](const logical::Aggregation& a) { CollectGroupTables(a.source, out, seen); },
      [&](const logical::PartialAggregation& a) { CollectGroupTables(a.source, out, seen); },
      [&](const logical::FinalAggregation& a) { CollectGroupTables(a.source, out, seen); },
      [&](const logical::CrossJoin& j) {
        CollectGroupTables(j.lhs, out, seen);
        CollectGroupTables(j.rhs, out, seen);
      },
      [&](const logical::Join& j) {
        CollectGroupTables(j.lhs, out, seen);
        CollectGroupTables(j.rhs, out, seen);
      },
  }, (*exprs.begin())->root_operator);
}

}  // namespace

std::unordered_set<std::string> GroupTables(utils::NotNull<Group*> g) {
  std::unordered_set<std::string> out;
  std::unordered_set<Group*> seen;
  CollectGroupTables(g, out, seen);
  return out;
}

}  // namespace stewkk::sql
