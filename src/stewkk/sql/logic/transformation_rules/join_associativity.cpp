#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <vector>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

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
      [&](const IntConst&) {},
      [&](const Literal&) {},
  }, e);
}

// Equivalent logical exprs in a memo group all expose the same output tables,
// so one expression suffices. Recurses across child groups via their fronts.
void CollectGroupTables(utils::NotNull<Group*> g, std::unordered_set<std::string>& out,
                        std::unordered_set<Group*>& seen) {
  if (!seen.insert(g.get()).second) return;
  auto exprs = g->GetLogicalExprs();
  if (exprs.empty()) return;
  std::visit(utils::Overloaded{
      [&](const logical::Table& t) { out.insert(t.name); },
      [&](const logical::Filter& f) { CollectGroupTables(f.source, out, seen); },
      [&](const logical::Projection& p) { CollectGroupTables(p.source, out, seen); },
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

std::unordered_set<std::string> GroupTables(utils::NotNull<Group*> g) {
  std::unordered_set<std::string> out;
  std::unordered_set<Group*> seen;
  CollectGroupTables(g, out, seen);
  return out;
}

}  // namespace

bool JoinAssociativity::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
  const auto& outer = std::get<logical::Join>(expr->root_operator);
  for (auto inner_expr : outer.lhs->GetLogicalExprs()) {
    if (std::holds_alternative<logical::Join>(inner_expr->root_operator)) return true;
  }
  return false;
}

// (A ⋈_p1 B) ⋈_p2 C  →  A ⋈_pa (B ⋈_pbc C)
// where pbc collects every conjunct of (p1 ∧ p2) whose attributes lie inside
// B ∪ C, and pa keeps the rest. Avoids the degenerate ⋈_TRUE inner that drops
// otherwise-pushable predicates onto the outer join.
LogicalOperator JoinAssociativity::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
  const auto& outer = std::get<logical::Join>(expr->root_operator);
  for (auto inner_expr : outer.lhs->GetLogicalExprs()) {
    if (!std::holds_alternative<logical::Join>(inner_expr->root_operator)) continue;
    const auto& inner = std::get<logical::Join>(inner_expr->root_operator);

    auto bc_tables = GroupTables(inner.rhs);
    auto c_tables = GroupTables(outer.rhs);
    bc_tables.insert(c_tables.begin(), c_tables.end());

    std::vector<Expression> conjs;
    CollectConjuncts(inner.qual, conjs);
    CollectConjuncts(outer.qual, conjs);

    std::vector<Expression> inner_quals;
    std::vector<Expression> outer_quals;
    for (auto& q : conjs) {
      std::unordered_set<std::string> q_tables;
      CollectAttrTables(q, q_tables);
      bool fits_inner = std::all_of(q_tables.begin(), q_tables.end(),
                                    [&](const auto& t) { return bc_tables.contains(t); });
      (fits_inner ? inner_quals : outer_quals).push_back(std::move(q));
    }

    auto new_rhs = memo.AddGroup(logical::Join{
        inner.rhs, outer.rhs, outer.type, AndConjuncts(inner_quals)})->group;
    // FIXME: should return more than one operators!!!
    return logical::Join{inner.lhs, new_rhs, inner.type, AndConjuncts(outer_quals)};
  }
  throw std::runtime_error{"cant perform JoinAssociativity"};
}

}  // namespace stewkk::sql
