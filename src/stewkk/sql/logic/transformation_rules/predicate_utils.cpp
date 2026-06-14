#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

#include <algorithm>
#include <format>
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

namespace {

std::shared_ptr<Expression> Share(Expression e) {
  return std::make_shared<Expression>(std::move(e));
}

std::string CanonicalKey(const Expression& e);

std::string CanonicalKey(const BinaryExpression& e) {
  return std::format("({} {} {})", static_cast<int>(e.binop),
                     CanonicalKey(*e.lhs), CanonicalKey(*e.rhs));
}

std::string CanonicalKey(const UnaryExpression& e) {
  return std::format("({} {})", static_cast<int>(e.op), CanonicalKey(*e.child));
}

std::string CanonicalKey(const InExpression& e) {
  std::string out = std::format("({} {}", e.negated ? "notin" : "in", CanonicalKey(*e.lhs));
  for (const auto& value : e.values) {
    out += ' ';
    out += CanonicalKey(value);
  }
  out += ')';
  return out;
}

std::string CanonicalKey(const AggregateExpression& e) {
  if (e.is_star) {
    return std::format("({} *)", static_cast<int>(e.function));
  }
  return std::format("({} {})", static_cast<int>(e.function), CanonicalKey(*e.argument));
}

std::string CanonicalKey(const Expression& e) {
  return std::visit(utils::Overloaded{
      [](const BinaryExpression& b) { return CanonicalKey(b); },
      [](const Attribute& a) { return std::format("(attr {} {})", a.table, a.name); },
      [](const IntConst& i) { return std::format("(int {})", i); },
      [](const StringConst& s) { return std::format("(str {})", s); },
      [](const UnaryExpression& u) { return CanonicalKey(u); },
      [](const InExpression& i) { return CanonicalKey(i); },
      [](const AggregateExpression& a) { return CanonicalKey(a); },
      [](const Literal& l) { return std::format("(literal {})", static_cast<int>(l)); },
  }, e);
}

void CollectByOp(const Expression& e, BinaryOp op, std::vector<Expression>& out) {
  if (const auto* b = std::get_if<BinaryExpression>(&e); b && b->binop == op) {
    CollectByOp(*b->lhs, op, out);
    CollectByOp(*b->rhs, op, out);
    return;
  }
  out.push_back(CanonicalizePredicate(e));
}

Expression ChainByOp(std::vector<Expression> exprs, BinaryOp op) {
  std::ranges::sort(exprs, [](const Expression& lhs, const Expression& rhs) {
    return CanonicalKey(lhs) < CanonicalKey(rhs);
  });
  Expression acc = std::move(exprs.front());
  for (size_t i = 1; i < exprs.size(); ++i) {
    acc = BinaryExpression{Share(std::move(acc)), op, Share(std::move(exprs[i]))};
  }
  return acc;
}

}  // namespace

Expression CanonicalizePredicate(const Expression& e) {
  return std::visit(utils::Overloaded{
      [](const BinaryExpression& b) -> Expression {
        if (b.binop == BinaryOp::kAnd || b.binop == BinaryOp::kOr) {
          std::vector<Expression> terms;
          CollectByOp(*b.lhs, b.binop, terms);
          CollectByOp(*b.rhs, b.binop, terms);
          return ChainByOp(std::move(terms), b.binop);
        }
        return BinaryExpression{
            Share(CanonicalizePredicate(*b.lhs)),
            b.binop,
            Share(CanonicalizePredicate(*b.rhs)),
        };
      },
      [](const UnaryExpression& u) -> Expression {
        return UnaryExpression{u.op, Share(CanonicalizePredicate(*u.child))};
      },
      [](const InExpression& i) -> Expression {
        std::vector<Expression> values;
        values.reserve(i.values.size());
        for (const auto& value : i.values) {
          values.push_back(CanonicalizePredicate(value));
        }
        std::ranges::sort(values, [](const Expression& lhs, const Expression& rhs) {
          return CanonicalKey(lhs) < CanonicalKey(rhs);
        });
        return InExpression{Share(CanonicalizePredicate(*i.lhs)), std::move(values), i.negated};
      },
      [](const AggregateExpression& a) -> Expression {
        if (a.is_star || !a.argument) return a;
        return AggregateExpression{a.function, Share(CanonicalizePredicate(*a.argument)), a.is_star};
      },
      [](const auto& leaf) -> Expression { return leaf; },
  }, e);
}

bool EquivalentPredicate(const Expression& lhs, const Expression& rhs) {
  return CanonicalizePredicate(lhs) == CanonicalizePredicate(rhs);
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
