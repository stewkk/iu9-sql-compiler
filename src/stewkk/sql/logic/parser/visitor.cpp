#include <stewkk/sql/logic/parser/visitor.hpp>

#include <algorithm>
#include <cctype>
#include <format>
#include <ranges>

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/result/error.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {

namespace {

struct ParsedTarget {
  Expression expression;
  std::optional<std::string> alias;
};

std::string UnquoteStandardString(std::string_view text) {
  if (text.size() < 2 || text.front() != '\'' || text.back() != '\'') {
    throw Error{ErrorType::kQueryNotSupported, "only standard single-quoted strings are supported"};
  }

  std::string result;
  result.reserve(text.size() - 2);
  for (size_t i = 1; i + 1 < text.size(); ++i) {
    if (text[i] == '\'' && i + 1 < text.size() - 1 && text[i + 1] == '\'') {
      result.push_back('\'');
      ++i;
      continue;
    }
    result.push_back(text[i]);
  }
  return result;
}

Operator GetOperatorWithChild(Operator&& op, Operator&& child) {
  struct ChildSetter {
    Operator operator() (Table&& parent) {
      std::unreachable();
    }
    Operator operator() (Projection&& parent) {
      parent.source = std::make_shared<Operator>(std::move(child));
      return parent;
    }
    Operator operator() (Filter&& filter) {
      filter.source = std::make_shared<Operator>(std::move(child));
      return filter;
    }
    Operator operator() (Aggregation&& aggregation) {
      aggregation.source = std::make_shared<Operator>(std::move(child));
      return aggregation;
    }
    Operator operator() (CrossJoin&& cross_join) {
      std::unreachable();
    }
    Operator operator() (Join&& cross_join) {
      std::unreachable();
    }

    Operator&& child;
  };
  return std::visit(ChildSetter{std::move(child)}, std::move(op));
}

std::string ToLower(std::string s) {
  std::ranges::transform(s, s.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return s;
}

bool ContainsAggregate(const Expression& expr) {
  struct Visitor {
    bool operator()(const BinaryExpression& expr) const {
      return ContainsAggregate(*expr.lhs) || ContainsAggregate(*expr.rhs);
    }
    bool operator()(const UnaryExpression& expr) const {
      return ContainsAggregate(*expr.child);
    }
    bool operator()(const InExpression& expr) const {
      return ContainsAggregate(*expr.lhs)
          || std::ranges::any_of(expr.values, [](const Expression& value) {
               return ContainsAggregate(value);
             });
    }
    bool operator()(const AggregateExpression&) const {
      return true;
    }
    bool operator()(const Attribute&) const { return false; }
    bool operator()(const IntConst&) const { return false; }
    bool operator()(const StringConst&) const { return false; }
    bool operator()(const Literal&) const { return false; }
  };
  return std::visit(Visitor{}, expr);
}

void CollectAggregates(const Expression& expr, std::vector<Expression>& out) {
  struct Visitor {
    void operator()(const BinaryExpression& expr) const {
      CollectAggregates(*expr.lhs, out);
      CollectAggregates(*expr.rhs, out);
    }
    void operator()(const UnaryExpression& expr) const {
      CollectAggregates(*expr.child, out);
    }
    void operator()(const InExpression& expr) const {
      CollectAggregates(*expr.lhs, out);
      for (const auto& value : expr.values) {
        CollectAggregates(value, out);
      }
    }
    void operator()(const AggregateExpression& expr) const {
      out.push_back(Expression{expr});
    }
    void operator()(const Attribute&) const {}
    void operator()(const IntConst&) const {}
    void operator()(const StringConst&) const {}
    void operator()(const Literal&) const {}

    std::vector<Expression>& out;
  };
  std::visit(Visitor{out}, expr);
}

std::vector<Expression> CollectAggregates(const std::vector<Expression>& expressions) {
  std::vector<Expression> result;
  for (const auto& expr : expressions) {
    CollectAggregates(expr, result);
  }
  return result;
}

Expression ReplaceAggregatesWithAttributes(const Expression& expr, size_t& counter);

Expression ReplaceAggregatesWithAttributes(const Expression& expr, size_t& counter) {
  struct Visitor {
    Expression operator()(const BinaryExpression& e) const {
      return BinaryExpression{
          std::make_shared<Expression>(ReplaceAggregatesWithAttributes(*e.lhs, counter)),
          e.binop,
          std::make_shared<Expression>(ReplaceAggregatesWithAttributes(*e.rhs, counter)),
      };
    }
    Expression operator()(const UnaryExpression& e) const {
      return UnaryExpression{
          e.op,
          std::make_shared<Expression>(ReplaceAggregatesWithAttributes(*e.child, counter)),
      };
    }
    Expression operator()(const InExpression& e) const {
      auto new_lhs = std::make_shared<Expression>(ReplaceAggregatesWithAttributes(*e.lhs, counter));
      std::vector<Expression> new_values;
      for (const auto& v : e.values) {
        new_values.push_back(ReplaceAggregatesWithAttributes(v, counter));
      }
      return InExpression{std::move(new_lhs), std::move(new_values), e.negated};
    }
    Expression operator()(const AggregateExpression&) const {
      return Attribute{"", std::format("__agg{}", counter++)};
    }
    Expression operator()(const Attribute& a) const { return a; }
    Expression operator()(const IntConst& c) const { return c; }
    Expression operator()(const StringConst& s) const { return s; }
    Expression operator()(const Literal& l) const { return l; }

    size_t& counter;
  };
  return std::visit(Visitor{counter}, expr);
}

std::vector<Expression> ReplaceAggregatesWithAttributes(const std::vector<Expression>& exprs) {
  std::vector<Expression> result;
  size_t counter = 0;
  for (const auto& expr : exprs) {
    result.push_back(ReplaceAggregatesWithAttributes(expr, counter));
  }
  return result;
}

} // namespace

std::any Visitor::visitStmt(codegen::PostgreSQLParser::StmtContext *ctx) {
  if (!ctx->children.empty() && !ctx->selectstmt()) {
    auto& rule_names = parser_->getRuleNames();
    std::vector<std::string_view> unsupported_rules;
    for (auto child : ctx->children) {
      if (!child) {
        continue;
      }
      if (antlr4::RuleContext* rule_context = dynamic_cast<antlr4::RuleContext*>(child)) {
        size_t rule_index = rule_context->getRuleIndex();
        unsupported_rules.emplace_back(rule_names[rule_index]);
      }
    }
    std::string rules_list
        = unsupported_rules | std::views::join_with(',') | std::ranges::to<std::string>();
    if (rules_list.empty()) {
      throw Error{ErrorType::kQueryNotSupported, "query is not supported"};
    }
    // NOTE: concatenation of errors could be useful here
    throw Error{ErrorType::kQueryNotSupported,
                std::format("{} {} currently unsupported", std::move(rules_list),
                            unsupported_rules.size() == 1 ? "is" : "are")};
  }
  return visitChildren(ctx);
}

std::any Visitor::visit(antlr4::tree::ParseTree *tree) {
  std::string rule_name;
  int indentation = 0;
  auto& rule_names = parser_->getRuleNames();
  if (antlr4::RuleContext *rule_context = dynamic_cast<antlr4::RuleContext *>(tree)) {
    size_t rule_index = rule_context->getRuleIndex();
    rule_name = rule_names[rule_index];
    indentation = rule_context->depth()-1;
  }
  std::string indentation_str(indentation*4, ' ');
#ifdef DEBUG
  std::clog << std::format("{}visiting rule: {}\n", indentation_str, rule_name);
#endif
  auto tmp = codegen::PostgreSQLParserBaseVisitor::visit(tree);
#ifdef DEBUG
  std::clog << std::format("{}exiting rule: {}\n", indentation_str, rule_name);
#endif
  return tmp;
}

std::any Visitor::visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) {
  // NOTE: column reference has form: database_name.schema_name.table_name.column_name
  auto table = ctx->colid()->getText();
  if (ctx->indirection()) {
    auto column = ctx->indirection()->indirection_el(0)->attr_name()->getText();
    return Expression{Attribute{std::move(table), std::move(column)}};
  }
  return Expression{Attribute{"", std::move(table)}};
}

std::any Visitor::visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) {
  if (ctx->subquery_Op()) {
    throw Error{ErrorType::kQueryNotSupported, "subquery_Op is not supported"};
  }
  auto exprs = ctx->a_expr_like();
  if (exprs.size() == 1) {
    return visit(exprs.front());
  }
  auto lhs_expr = std::any_cast<Expression>(visit(exprs.front()));
  auto rhs_expr = std::any_cast<Expression>(visit(exprs.back()));
  BinaryOp op;
  if (ctx->LT()) {
    op = BinaryOp::kLt;
  } else if (ctx->GT()) {
    op = BinaryOp::kGt;
  } else if (ctx->EQUAL()) {
    op = BinaryOp::kEq;
  } else if (ctx->LESS_EQUALS()) {
    op = BinaryOp::kLe;
  } else if (ctx->GREATER_EQUALS()) {
    op = BinaryOp::kGe;
  } else if (ctx->NOT_EQUALS()) {
    op = BinaryOp::kNotEq;
  }
  return Expression{
    BinaryExpression {
      std::make_shared<Expression>(lhs_expr),
      op,
      std::make_shared<Expression>(rhs_expr),
    },
  };
}

Visitor::Visitor(codegen::PostgreSQLParser* parser) : parser_(parser) {}

std::any Visitor::visitRoot(codegen::PostgreSQLParser::RootContext *ctx) {
  return visit(ctx->stmtblock());
}

std::any Visitor::visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) {
  auto stmt = ctx->stmt();
  if (stmt.empty()) {
    return {};
  }
  return visit(ctx->stmt()[0]);
}

std::any Visitor::visitSimple_select_intersect(codegen::PostgreSQLParser::Simple_select_intersectContext *ctx) {
  // TODO: INTERSECT (with ALL or DISTINCT)
  return visit(ctx->simple_select_pramary().front());
}

std::any Visitor::visitTarget_label(codegen::PostgreSQLParser::Target_labelContext *ctx) {
  ParsedTarget target{
      .expression = std::any_cast<Expression>(visit(ctx->a_expr())),
  };
  if (ctx->colLabel()) {
    target.alias = ctx->colLabel()->getText();
  } else if (ctx->bareColLabel()) {
    target.alias = ctx->bareColLabel()->getText();
  }
  return target;
}

std::any Visitor::visitTarget_star(codegen::PostgreSQLParser::Target_starContext* ctx) {
  return {};
}

std::any Visitor::visitTarget_list(codegen::PostgreSQLParser::Target_listContext* ctx) {
  const auto& targets = ctx->target_el();
  auto parsed_targets
      = targets | std::views::transform([this](const auto& target) { return visit(target); })
        | std::views::filter(
            [](const std::any& expr) { return expr.has_value(); })
        | std::views::transform(
            [](std::any expr) { return std::any_cast<ParsedTarget>(expr); })
        | std::ranges::to<std::vector>();

  if (!parsed_targets.empty()) {
    std::vector<Expression> target_expressions;
    std::vector<std::optional<std::string>> aliases;
    target_expressions.reserve(parsed_targets.size());
    aliases.reserve(parsed_targets.size());
    for (auto& target : parsed_targets) {
      aliases.push_back(std::move(target.alias));
      target_expressions.push_back(std::move(target.expression));
    }
    if (std::ranges::none_of(aliases, [](const auto& alias) { return alias.has_value(); })) {
      aliases.clear();
    }
    return Operator{Projection{std::move(target_expressions), nullptr, std::move(aliases)}};
  }

  return {};
}

std::any Visitor::visitGroup_clause(codegen::PostgreSQLParser::Group_clauseContext *ctx) {
  return visit(ctx->group_by_list());
}

std::any Visitor::visitGroup_by_list(codegen::PostgreSQLParser::Group_by_listContext *ctx) {
  std::vector<Expression> result;
  for (auto* item : ctx->group_by_item()) {
    result.push_back(std::any_cast<Expression>(visit(item)));
  }
  return result;
}

std::any Visitor::visitGroup_by_item(codegen::PostgreSQLParser::Group_by_itemContext *ctx) {
  if (!ctx->a_expr()) {
    throw Error{ErrorType::kQueryNotSupported,
                "ROLLUP, CUBE, GROUPING SETS and empty GROUP BY items are not supported"};
  }
  return visit(ctx->a_expr());
}

std::any Visitor::visitFrom_clause(codegen::PostgreSQLParser::From_clauseContext *ctx) {
  return visit(ctx->from_list());
}

std::any Visitor::visitFrom_list(codegen::PostgreSQLParser::From_listContext *ctx) {
  auto table_ref = ctx->table_ref();
  auto result = std::any_cast<Operator>(visit(table_ref.front()));
  for (auto it = table_ref.begin()+1; it != table_ref.end(); it++) {
    auto rhs = std::any_cast<Operator>(visit(*it));
    result = CrossJoin{
        std::make_shared<Operator>(std::move(result)),
        std::make_shared<Operator>(std::move(rhs)),
    };
  }
  return result;
}

namespace {

using TableRefCtx = codegen::PostgreSQLParser::Table_refContext;
using ChildIt = std::vector<antlr4::tree::ParseTree*>::const_iterator;

Operator BuildTableRef(Visitor* v, TableRefCtx* ctx);

Expression MakeBinary(Expression lhs, BinaryOp op, Expression rhs) {
  return BinaryExpression{
      std::make_shared<Expression>(std::move(lhs)),
      op,
      std::make_shared<Expression>(std::move(rhs)),
  };
}

Expression MakeUnary(UnaryOp op, Expression child) {
  return UnaryExpression{op, std::make_shared<Expression>(std::move(child))};
}

std::pair<Operator, ChildIt> ExtractAtom(Visitor* v, TableRefCtx* ctx) {
  if (ctx->xmltable()) {
    throw Error{ErrorType::kQueryNotSupported, "xmltable is not supported"};
  }
  if (ctx->func_table()) {
    throw Error{ErrorType::kQueryNotSupported, "func_table is not supported"};
  }
  if (ctx->LATERAL_P()) {
    throw Error{ErrorType::kQueryNotSupported, "LATERAL clause is not supported"};
  }
  if (ctx->tablesample_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "tablesample_clause is not supported"};
  }

  const auto& children = ctx->children;
  auto it = children.cbegin();
  Operator res;
  if (ctx->relation_expr()) {
    auto table = Table{std::any_cast<std::string>(v->visit(ctx->relation_expr()))};
    if (auto* alias = ctx->alias_clause()) {
      if (alias->name_list()) {
        throw Error{ErrorType::kQueryNotSupported, "alias column lists are not supported"};
      }
      table.alias = alias->colid()->getText();
    }
    res = std::move(table);
    ++it;
    if (ctx->alias_clause()) {
      ++it;
    }
  } else if (ctx->select_with_parens()) {
    if (ctx->alias_clause()) {
      throw Error{ErrorType::kQueryNotSupported, "aliases on subqueries are not supported"};
    }
    res = std::any_cast<Operator>(v->visit(ctx->select_with_parens()));
    ++it;
  } else if (ctx->OPEN_PAREN()) {
    if (ctx->alias_clause()) {
      throw Error{ErrorType::kQueryNotSupported, "aliases on parenthesized table refs are not supported"};
    }
    res = BuildTableRef(v, ctx->table_ref(0));
    it += 3;
  }
  return {std::move(res), it};
}

Operator ContinueChain(Visitor* v, Operator lhs, TableRefCtx* ctx, ChildIt it) {
  const auto end = ctx->children.cend();
  while (it != end) {
    const auto text = (*it)->getText();
    if (text == "CROSS") {
      it += 2;
      auto* rhs_ctx = dynamic_cast<TableRefCtx*>(*it);
      ++it;
      auto [rhs_atom, rhs_post] = ExtractAtom(v, rhs_ctx);
      lhs = CrossJoin{
          std::make_shared<Operator>(std::move(lhs)),
          std::make_shared<Operator>(std::move(rhs_atom)),
      };
      lhs = ContinueChain(v, std::move(lhs), rhs_ctx, rhs_post);
    } else if (text == "NATURAL") {
      throw Error{ErrorType::kQueryNotSupported, "NATURAL clause is not supported"};
    } else {
      JoinType jt = JoinType::kInner;
      if (text == "JOIN") {
        ++it;
      } else {
        jt = std::any_cast<JoinType>(v->visit(*it));
        it += 2;
      }
      auto* rhs_ctx = dynamic_cast<TableRefCtx*>(*it);
      ++it;
      auto qual = std::any_cast<Expression>(v->visit(*it));
      ++it;
      auto [rhs_atom, rhs_post] = ExtractAtom(v, rhs_ctx);
      lhs = Join{
          jt,
          std::move(qual),
          std::make_shared<Operator>(std::move(lhs)),
          std::make_shared<Operator>(std::move(rhs_atom)),
      };
      lhs = ContinueChain(v, std::move(lhs), rhs_ctx, rhs_post);
    }
  }
  return lhs;
}

Operator BuildTableRef(Visitor* v, TableRefCtx* ctx) {
  auto [atom, it] = ExtractAtom(v, ctx);
  return ContinueChain(v, std::move(atom), ctx, it);
}

}  // namespace

std::any Visitor::visitTable_ref(codegen::PostgreSQLParser::Table_refContext *ctx) {
  return Operator{BuildTableRef(this, ctx)};
}

std::any Visitor::visitJoin_type(codegen::PostgreSQLParser::Join_typeContext *ctx) {
  if (ctx->INNER_P()) {
    return JoinType::kInner;
  }
  if (ctx->FULL()) {
    return JoinType::kFull;
  }
  if (ctx->LEFT()) {
    return JoinType::kLeft;
  }
  return JoinType::kRight;
}

std::any Visitor::visitJoin_qual(codegen::PostgreSQLParser::Join_qualContext *ctx) {
  if (ctx->USING()) {
    throw Error{ErrorType::kQueryNotSupported, "USING clause is not supported"};
  }
  return visit(ctx->a_expr());
}

std::any Visitor::visitRelation_expr(codegen::PostgreSQLParser::Relation_exprContext *ctx) {
  if (ctx->ONLY()) {
    throw Error{ErrorType::kQueryNotSupported, "ONLY clause is not supported"};
  }
  if (ctx->STAR()) {
    throw Error{ErrorType::kQueryNotSupported, "tablename * clause is not supported"};
  }
  return visit(ctx->qualified_name());
}

std::any Visitor::visitQualified_name(codegen::PostgreSQLParser::Qualified_nameContext *ctx) {
  if (ctx->indirection()) {
    throw Error{ErrorType::kQueryNotSupported, "bare column names are not supported"};
  }
  return ctx->colid()->getText();
}

std::any Visitor::visitSimple_select_pramary(
    codegen::PostgreSQLParser::Simple_select_pramaryContext *ctx) {
  if (ctx->select_with_parens()) {
    return visit(ctx->select_with_parens());
  }
  if (!ctx->SELECT()) {
    throw Error{ErrorType::kQueryNotSupported, "VALUES and TABLE clauses are not supported"};
  }
  if (ctx->distinct_clause()) {
    // TODO: support distinct clause
    throw Error{ErrorType::kQueryNotSupported, "DISTINCT clause is not supported"};
  }

  // NOTE: all_clause_ is ignored
  if (ctx->having_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "HAVING clause is not supported"};
  }
  if (ctx->window_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "WINDOW clause is not supported"};
  }

  Operator result = Table{kEmptyTableName};
  std::optional<Projection> projection;

  if (ctx->from_clause()) {
    result = std::any_cast<Operator>(visit(ctx->from_clause()));
  }

  if (ctx->where_clause()) {
    auto filter = std::any_cast<Operator>(visit(ctx->where_clause()));
    result = GetOperatorWithChild(std::move(filter), std::move(result));
  }

  if (ctx->target_list_()) {
    auto target_list_opt = visit(ctx->target_list_());
    if (target_list_opt.has_value()) {
      auto projection_op = std::any_cast<Operator>(target_list_opt);
      projection = std::get<Projection>(std::move(projection_op));
    }
  }

  std::vector<Expression> group_by;
  if (ctx->group_clause()) {
    group_by = std::any_cast<std::vector<Expression>>(visit(ctx->group_clause()));
  }

  std::vector<Expression> aggregates;
  if (projection) {
    aggregates = CollectAggregates(projection->expressions);
  }

  if (!group_by.empty() || !aggregates.empty()) {
    if (projection) {
      projection->expressions = ReplaceAggregatesWithAttributes(projection->expressions);
    }
    result = Aggregation{
        std::move(group_by),
        std::move(aggregates),
        std::make_shared<Operator>(std::move(result)),
    };
  }

  if (projection) {
    if (std::ranges::none_of(projection->aliases,
                             [](const auto& alias) { return alias.has_value(); })) {
      projection->aliases.clear();
    }
    result = Projection{
        std::move(projection->expressions),
        std::make_shared<Operator>(std::move(result)),
        std::move(projection->aliases),
    };
  }

  return result;
}

std::any Visitor::visitWhere_clause(codegen::PostgreSQLParser::Where_clauseContext *ctx) {
  auto where_expr = std::any_cast<Expression>(visit(ctx->a_expr()));
  return Operator{Filter{where_expr, nullptr}};
}

std::any Visitor::visitSelect_clause(codegen::PostgreSQLParser::Select_clauseContext *ctx) {
  // TODO: UNION and EXCEPT (with ALL or DISTINCT)
  return visit(ctx->simple_select_intersect().front());
}

std::any Visitor::visitSelect_with_parens(codegen::PostgreSQLParser::Select_with_parensContext *ctx) {
  if (ctx->select_with_parens()) {
    return visit(ctx->select_with_parens());
  }
  return visit(ctx->select_no_parens());
}

std::any Visitor::visitSelect_no_parens(codegen::PostgreSQLParser::Select_no_parensContext *ctx) {
  if (ctx->with_clause()) {
    // TODO
  }

  auto select_clause = std::any_cast<Operator>(visit(ctx->select_clause()));

  if (ctx->sort_clause_()) {
    auto* sortby_list = ctx->sort_clause_()->sort_clause()->sortby_list();
    std::vector<SortKey> keys;
    for (auto* sortby : sortby_list->sortby()) {
      keys.push_back(std::any_cast<SortKey>(visit(sortby)));
    }
    required_order_ = SortOrder{std::move(keys)};
  }

  return select_clause;
}

std::any Visitor::visitSortby(codegen::PostgreSQLParser::SortbyContext *ctx) {
  if (ctx->USING()) {
    throw Error{ErrorType::kQueryNotSupported, "USING in ORDER BY is not supported"};
  }
  if (ctx->nulls_order_()) {
    throw Error{ErrorType::kQueryNotSupported, "NULLS FIRST/LAST in ORDER BY is not supported"};
  }

  auto expr = std::any_cast<Expression>(visit(ctx->a_expr()));
  auto* attr = std::get_if<Attribute>(&expr);
  if (!attr) {
    throw Error{ErrorType::kQueryNotSupported, "ORDER BY expression must be a column reference"};
  }

  Direction dir = Direction::kAsc;
  if (ctx->asc_desc_()) {
    dir = std::any_cast<Direction>(visit(ctx->asc_desc_()));
  }

  return SortKey{attr->table, attr->name, dir};
}

std::any Visitor::visitAsc_desc_(codegen::PostgreSQLParser::Asc_desc_Context *ctx) {
  if (ctx->DESC()) {
    return Direction::kDesc;
  }
  return Direction::kAsc;
}

std::any Visitor::visitA_expr_qual(codegen::PostgreSQLParser::A_expr_qualContext *ctx) {
  if (ctx->qual_op()) {
    throw Error{ErrorType::kQueryNotSupported, "qualified operators are not supported"};
  }
  return visit(ctx->a_expr_lessless());
}

std::any Visitor::visitA_expr_lessless(codegen::PostgreSQLParser::A_expr_lesslessContext *ctx) {
  if (ctx->a_expr_or().size() > 1) {
    throw Error{ErrorType::kQueryNotSupported, "<< and >> operators are not supported"};
  }
  return visit(ctx->a_expr_or(0));
}

std::any Visitor::visitA_expr_or(codegen::PostgreSQLParser::A_expr_orContext *ctx) {
  const auto& exprs = ctx->a_expr_and();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  for (size_t i = 1; i < exprs.size(); i++) {
    auto tmp = std::any_cast<Expression>(visit(exprs[i]));
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kOr,
                              std::make_shared<Expression>(std::move(tmp))};
  }
  return result;
}

std::any Visitor::visitA_expr_and(codegen::PostgreSQLParser::A_expr_andContext *ctx) {
  const auto& exprs = ctx->a_expr_between();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  for (size_t i = 1; i < exprs.size(); i++) {
    auto tmp = std::any_cast<Expression>(visit(exprs[i]));
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kAnd,
                              std::make_shared<Expression>(std::move(tmp))};
  }
  return result;
}

std::any Visitor::visitA_expr_between(codegen::PostgreSQLParser::A_expr_betweenContext *ctx) {
  if (!ctx->BETWEEN()) {
    return visit(ctx->a_expr_in(0));
  }
  if (ctx->SYMMETRIC()) {
    throw Error{ErrorType::kQueryNotSupported, "BETWEEN SYMMETRIC clause is not supported"};
  }

  auto exprs = ctx->a_expr_in();
  auto value = std::any_cast<Expression>(visit(exprs[0]));
  auto lower = std::any_cast<Expression>(visit(exprs[1]));
  auto upper = std::any_cast<Expression>(visit(exprs[2]));

  auto ge = MakeBinary(value, BinaryOp::kGe, std::move(lower));
  auto le = MakeBinary(std::move(value), BinaryOp::kLe, std::move(upper));
  auto result = MakeBinary(std::move(ge), BinaryOp::kAnd, std::move(le));
  if (ctx->NOT()) {
    result = MakeUnary(UnaryOp::kNot, std::move(result));
  }
  return result;
}

std::any Visitor::visitA_expr_in(codegen::PostgreSQLParser::A_expr_inContext *ctx) {
  auto lhs = std::any_cast<Expression>(visit(ctx->a_expr_unary_not()));
  if (!ctx->IN_P()) {
    return lhs;
  }

  auto* in_list = dynamic_cast<codegen::PostgreSQLParser::In_expr_listContext*>(ctx->in_expr());
  if (!in_list) {
    throw Error{ErrorType::kQueryNotSupported, "IN subqueries are not supported"};
  }

  std::vector<Expression> values;
  for (auto* expr : in_list->expr_list()->a_expr()) {
    values.push_back(std::any_cast<Expression>(visit(expr)));
  }
  return Expression{InExpression{
      std::make_shared<Expression>(std::move(lhs)),
      std::move(values),
      ctx->NOT() != nullptr,
  }};
}

std::any Visitor::visitA_expr_unary_not(codegen::PostgreSQLParser::A_expr_unary_notContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_isnull()));
  if (ctx->NOT()) {
    result = UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_isnull(codegen::PostgreSQLParser::A_expr_isnullContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_is_not()));
  if (ctx->ISNULL()) {
    result = UnaryExpression{UnaryOp::kIsNull, std::make_shared<Expression>(std::move(result))};
  }
  if (ctx->NOTNULL()) {
    result = UnaryExpression{UnaryOp::kIsNull, std::make_shared<Expression>(std::move(result))};
    result = UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_is_not(codegen::PostgreSQLParser::A_expr_is_notContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_compare()));
  if (!ctx->IS()) {
    return result;
  }

  if (ctx->NULL_P()) {
    result = UnaryExpression{UnaryOp::kIsNull, std::make_shared<Expression>(std::move(result))};
  } else if (ctx->TRUE_P()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kTrue)};
  } else if (ctx->FALSE_P()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kFalse)};
  } else if (ctx->UNKNOWN()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kUnknown)};
  } else if (ctx->DISTINCT()) {
    throw Error{ErrorType::kQueryNotSupported, "IS DISTINCT FROM clause is not supported"};
  } else if (ctx->OF()) {
    throw Error{ErrorType::kQueryNotSupported, "IS OF (type_list) clause is not supported"};
  } else if (ctx->DOCUMENT_P()) {
    throw Error{ErrorType::kQueryNotSupported, "IS DOCUMENT clause is not supported"};
  } else if (ctx->NORMALIZED()) {
    throw Error{ErrorType::kQueryNotSupported, "IS NORMALIZED clause is not supported"};
  }

  if (ctx->NOT()) {
    result = UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_like(codegen::PostgreSQLParser::A_expr_likeContext *ctx) {
  const auto& exprs = ctx->a_expr_qual_op();
  if (exprs.size() > 1) {
    // NOTE: may want to implement
    throw Error{ErrorType::kQueryNotSupported, "LIKE clause is not supported"};
  }
  return visit(exprs.front());
}

std::any Visitor::visitA_expr_qual_op(codegen::PostgreSQLParser::A_expr_qual_opContext *ctx) {
  const auto& exprs = ctx->a_expr_unary_qualop();
  if (exprs.size() == 1) {
    return visit(exprs.front());
  }
  if (exprs.size() == 2 && ctx->qual_op(0)->getText() == "!=") {
    auto lhs = std::any_cast<Expression>(visit(exprs[0]));
    auto rhs = std::any_cast<Expression>(visit(exprs[1]));
    return Expression{BinaryExpression{
        std::make_shared<Expression>(std::move(lhs)),
        BinaryOp::kNotEq,
        std::make_shared<Expression>(std::move(rhs)),
    }};
  }
  throw Error{ErrorType::kQueryNotSupported, "qual_op is not supported"};
}

std::any Visitor::visitA_expr_unary_qualop(codegen::PostgreSQLParser::A_expr_unary_qualopContext *ctx) {
  if (ctx->qual_op()) {
    // NOTE: may want to implement
    throw Error{ErrorType::kQueryNotSupported, "unary_qual_op is not supported"};
  }
  return visit(ctx->a_expr_add());
}

std::any Visitor::visitA_expr_add(codegen::PostgreSQLParser::A_expr_addContext *ctx) {
  const auto& exprs = ctx->a_expr_mul();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  auto expr_it = exprs.cbegin()+1;

  const auto& children = ctx->children;
  auto op_it = children.cbegin()+1;

  while (expr_it != exprs.cend()) {
    auto op = (*op_it)->getText();
    auto rhs = std::any_cast<Expression>(visit(*expr_it));
    if (op == "+") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kPlus,
          std::make_shared<Expression>(std::move(rhs)),
      };
    } else if (op == "-") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kMinus,
          std::make_shared<Expression>(std::move(rhs)),
      };
    }
    op_it += 2;
    expr_it++;
  }
  return result;
}

std::any Visitor::visitA_expr_mul(codegen::PostgreSQLParser::A_expr_mulContext *ctx) {
  const auto& exprs = ctx->a_expr_caret();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  auto expr_it = exprs.cbegin()+1;

  const auto& children = ctx->children;
  auto op_it = children.cbegin()+1;

  while (expr_it != exprs.cend()) {
    auto op = (*op_it)->getText();
    auto rhs = std::any_cast<Expression>(visit(*expr_it));
    if (op == "*") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kMul,
          std::make_shared<Expression>(std::move(rhs)),
      };
    } else if (op == "/") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kDiv,
          std::make_shared<Expression>(std::move(rhs)),
      };
    } else if (op == "%") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kMod,
          std::make_shared<Expression>(std::move(rhs)),
      };
    }
    op_it += 2;
    expr_it++;
  }
  return result;
}

std::any Visitor::visitA_expr_caret(codegen::PostgreSQLParser::A_expr_caretContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_unary_sign(0)));
  if (ctx->CARET()) {
    auto rhs = std::any_cast<Expression>(visit(ctx->a_expr_unary_sign(1)));
    result = BinaryExpression{
        std::make_shared<Expression>(std::move(result)),
        BinaryOp::kPow,
        std::make_shared<Expression>(std::move(rhs)),
    };
  }
  return result;
}

std::any Visitor::visitA_expr_unary_sign(codegen::PostgreSQLParser::A_expr_unary_signContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_at_time_zone()));
  if (ctx->MINUS()) {
    result = UnaryExpression{UnaryOp::kMinus, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_at_time_zone(codegen::PostgreSQLParser::A_expr_at_time_zoneContext *ctx) {
  if (ctx->AT()) {
    throw Error{ErrorType::kQueryNotSupported, "AT TIME ZONE literal is not supported"};
  }
  return visit(ctx->a_expr_collate());
}

std::any Visitor::visitA_expr_collate(codegen::PostgreSQLParser::A_expr_collateContext *ctx) {
  if (ctx->COLLATE()) {
    throw Error{ErrorType::kQueryNotSupported, "COLLATE clause is not supported"};
  }
  return visit(ctx->a_expr_typecast());
}

std::any Visitor::visitA_expr_typecast(codegen::PostgreSQLParser::A_expr_typecastContext *ctx) {
  if (!ctx->TYPECAST().empty()) {
    // NOTE: may want to support
    throw Error{ErrorType::kQueryNotSupported, "TYPECAST clause is not supported"};
  }
  return visit(ctx->c_expr());
}

std::any Visitor::visitC_expr_exists(codegen::PostgreSQLParser::C_expr_existsContext *ctx) {
  // NOTE: may want to support
  throw Error{ErrorType::kQueryNotSupported, "EXISTS clause is not supported"};
}

std::any Visitor::visitC_expr_expr(codegen::PostgreSQLParser::C_expr_exprContext *ctx) {
  if (ctx->columnref()) {
    return visit(ctx->columnref());
  }
  if (ctx->aexprconst()) {
    return visit(ctx->aexprconst());
  }
  if (ctx->a_expr_in_parens != nullptr) {
    return visit(ctx->a_expr_in_parens);
  }
  if (ctx->func_expr()) {
    return visit(ctx->func_expr());
  }
  // NOTE: may want to support
  throw Error{ErrorType::kQueryNotSupported, "c_expr is not fully supported"};
}

std::any Visitor::visitFunc_expr(codegen::PostgreSQLParser::Func_exprContext *ctx) {
  if (ctx->within_group_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "WITHIN GROUP clause is not supported"};
  }
  if (ctx->filter_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "aggregate FILTER clause is not supported"};
  }
  if (ctx->over_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "window functions are not supported"};
  }
  if (!ctx->func_application()) {
    throw Error{ErrorType::kQueryNotSupported, "function expression is not supported"};
  }
  return visit(ctx->func_application());
}

std::any Visitor::visitFunc_application(codegen::PostgreSQLParser::Func_applicationContext *ctx) {
  if (ctx->VARIADIC()) {
    throw Error{ErrorType::kQueryNotSupported, "VARIADIC function arguments are not supported"};
  }
  if (ctx->ALL()) {
    throw Error{ErrorType::kQueryNotSupported, "ALL in aggregate calls is not supported"};
  }
  if (ctx->DISTINCT()) {
    throw Error{ErrorType::kQueryNotSupported, "DISTINCT in aggregate calls is not supported"};
  }
  if (ctx->sort_clause_()) {
    throw Error{ErrorType::kQueryNotSupported, "ORDER BY in aggregate calls is not supported"};
  }

  auto name = ToLower(ctx->func_name()->getText());
  AggregateFunction function;
  if (name == "sum") {
    function = AggregateFunction::kSum;
  } else if (name == "count") {
    function = AggregateFunction::kCount;
  } else {
    throw Error{ErrorType::kQueryNotSupported,
                std::format("function {} is not supported", ctx->func_name()->getText())};
  }

  if (ctx->STAR()) {
    if (function != AggregateFunction::kCount) {
      throw Error{ErrorType::kQueryNotSupported, "only COUNT(*) supports star arguments"};
    }
    return Expression{AggregateExpression{function, nullptr, true}};
  }

  auto* args = ctx->func_arg_list();
  if (!args || args->func_arg_expr().size() != 1) {
    throw Error{ErrorType::kQueryNotSupported,
                std::format("{} requires exactly one argument", ToString(function))};
  }

  auto argument = std::any_cast<Expression>(visit(args->func_arg_expr(0)));
  if (ContainsAggregate(argument)) {
    throw Error{ErrorType::kQueryNotSupported, "nested aggregate calls are not supported"};
  }

  return Expression{AggregateExpression{
      function,
      std::make_shared<Expression>(std::move(argument)),
      false,
  }};
}

std::any Visitor::visitFunc_arg_expr(codegen::PostgreSQLParser::Func_arg_exprContext *ctx) {
  if (ctx->param_name()) {
    throw Error{ErrorType::kQueryNotSupported, "named function arguments are not supported"};
  }
  return visit(ctx->a_expr());
}

std::any Visitor::visitC_expr_case(codegen::PostgreSQLParser::C_expr_caseContext *ctx) {
  // NOTE: may want to support
  throw Error{ErrorType::kQueryNotSupported, "CASE clause is not supported"};
}

std::any Visitor::visitAexprconst(codegen::PostgreSQLParser::AexprconstContext *ctx) {
  if (ctx->constinterval()) {
    throw Error{ErrorType::kQueryNotSupported, "intervals are not supported"};
  }
  if (ctx->sconst()) {
    auto* sconst = ctx->sconst();
    if (sconst->uescape_()) {
      throw Error{ErrorType::kQueryNotSupported, "UESCAPE strings are not supported"};
    }
    auto* any = sconst->anysconst();
    if (!any->StringConstant()) {
      throw Error{ErrorType::kQueryNotSupported, "only standard single-quoted strings are supported"};
    }
    return Expression{StringConst{UnquoteStandardString(any->StringConstant()->getText())}};
  }
  if (ctx->xconst()) {
    throw Error{ErrorType::kQueryNotSupported, "hex literals are not supported"};
  }
  if (ctx->bconst()) {
    throw Error{ErrorType::kQueryNotSupported, "binary literals are not supported"};
  }
  if (ctx->fconst()) {
    throw Error{ErrorType::kQueryNotSupported, "float literals are not supported"};
  }
  if (ctx->TRUE_P()) {
    return Expression{Literal::kTrue};
  }
  if (ctx->FALSE_P()) {
    return Expression{Literal::kFalse};
  }
  if (ctx->NULL_P()) {
    return Expression{Literal::kNull};
  }
  if (ctx->iconst()) {
    return visit(ctx->iconst());
  }
  throw Error{ErrorType::kUnknown, "some literal is not supported"};
}

std::any Visitor::visitIconst(codegen::PostgreSQLParser::IconstContext *ctx) {
  if (!ctx->Integral()) {
    throw Error{ErrorType::kQueryNotSupported, "binary, octal and hexadecimal constants are not supported"};
  }

  std::stringstream ss(ctx->Integral()->getText());
  int64_t value;

  if (ss >> value) {
    return Expression{IntConst{value}};
  } else {
    throw Error{ErrorType::kConversionError, std::format("integer literal \"{}\" cannot be converted to 64-bit signed integer", ctx->Integral()->getText())};
  }
}

}  // namespace stewkk::sql
