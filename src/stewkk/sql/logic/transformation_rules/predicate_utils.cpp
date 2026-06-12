#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

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

std::unordered_set<std::string> ExprTables(const Expression& e) {
  std::unordered_set<std::string> out;
  CollectAttrTables(e, out);
  return out;
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
