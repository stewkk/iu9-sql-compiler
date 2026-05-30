#include <stewkk/sql/logic/transformation_rules/in_to_or_chain.hpp>

#include <memory>
#include <utility>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

bool ContainsIn(const Expression& e) {
  return std::visit(utils::Overloaded{
      [](const BinaryExpression& b) { return ContainsIn(*b.lhs) || ContainsIn(*b.rhs); },
      [](const UnaryExpression& u) { return ContainsIn(*u.child); },
      [](const InExpression&) { return true; },
      [](const AggregateExpression& a) {
        return !a.is_star && a.argument && ContainsIn(*a.argument);
      },
      [](const Attribute&) { return false; },
      [](const IntConst&) { return false; },
      [](const StringConst&) { return false; },
      [](const Literal&) { return false; },
  }, e);
}

std::shared_ptr<Expression> Share(Expression e) {
  return std::make_shared<Expression>(std::move(e));
}

// Expand a single InExpression into a left-associative chain. IN folds with OR
// over `lhs = value_i`; NOT IN folds with AND over `lhs != value_i`. The
// associativity and operand order match converter.py's OR/AND fold so the
// reachability target plan compares equal.
Expression ExpandIn(const InExpression& in);

Expression Expand(const Expression& e) {
  return std::visit(utils::Overloaded{
      [](const BinaryExpression& b) -> Expression {
        return BinaryExpression{Share(Expand(*b.lhs)), b.binop, Share(Expand(*b.rhs))};
      },
      [](const UnaryExpression& u) -> Expression {
        return UnaryExpression{u.op, Share(Expand(*u.child))};
      },
      [](const InExpression& in) -> Expression { return ExpandIn(in); },
      [](const AggregateExpression& a) -> Expression {
        if (a.is_star || !a.argument) return a;
        return AggregateExpression{a.function, Share(Expand(*a.argument)), a.is_star};
      },
      [](const auto& leaf) -> Expression { return leaf; },
  }, e);
}

Expression ExpandIn(const InExpression& in) {
  Expression lhs = Expand(*in.lhs);
  BinaryOp leaf_op = in.negated ? BinaryOp::kNotEq : BinaryOp::kEq;
  BinaryOp join_op = in.negated ? BinaryOp::kAnd : BinaryOp::kOr;

  Expression acc = BinaryExpression{Share(lhs), leaf_op, Share(Expand(in.values.front()))};
  for (size_t i = 1; i < in.values.size(); ++i) {
    Expression cmp = BinaryExpression{Share(lhs), leaf_op, Share(Expand(in.values[i]))};
    acc = BinaryExpression{Share(std::move(acc)), join_op, Share(std::move(cmp))};
  }
  return acc;
}

}  // namespace

bool InToOrChain::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  return ContainsIn(f.predicate);
}

LogicalOperator InToOrChain::ApplyImpl(utils::NotNull<LogicalExpr*> expr, Memo& memo) {
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  return logical::Filter{f.source, Expand(f.predicate)};
}

}  // namespace stewkk::sql
