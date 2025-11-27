#include <stewkk/sql/logic/parser/visitor.hpp>

#include <ranges>

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/result/error.hpp>

namespace stewkk::sql {

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
  std::cout << std::format("{}visiting rule: {}\n", indentation_str, rule_name);
  auto tmp = codegen::PostgreSQLParserBaseVisitor::visit(tree);
  std::cout << std::format("{}exiting rule: {}\n", indentation_str, rule_name);
  return tmp;
}

std::any Visitor::visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) {
  auto table = ctx->colid()->getText();
  auto column = ctx->indirection()->indirection_el(0)->attr_name()->getText();
  return Attribute{std::move(table), std::move(column)};
}

std::any Visitor::visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) {
  auto lhs = ctx->a_expr_like(0);
  auto rhs = ctx->a_expr_like(1);
  if (!lhs || !rhs) {
    return visitChildren(ctx);
  }
  auto lhs_any = visit(lhs);
  if (Attribute* attr = std::any_cast<Attribute>(&lhs_any)) {
    return Expression{
        BinaryExpression{std::make_shared<Expression>(*attr), BinaryOp::kGt,
                         std::make_shared<Expression>(IntConst{std::stoi(rhs->getText())})}};
  }
  std::unreachable();
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

std::any Visitor::visitTarget_list(codegen::PostgreSQLParser::Target_listContext *ctx) {
  std::vector<Attribute> targets;
  if (!(ctx->target_el().size() == 1 && ctx->target_el(0)->getText() == "*")) {
    targets = ctx->target_el()
              | std::ranges::views::transform([this](const auto &target_node) {
                  auto expr_any = visit(target_node);
                  if (Attribute *attr = std::any_cast<Attribute>(&expr_any)) {
                    return *attr;
                  }
                  // TODO: support arbitrary expressions here
                  std::unreachable();
                })
              | std::ranges::to<std::vector>();
  }

  if (!targets.empty()) {
    return Projection{targets, nullptr};
  }

  return {};
}

std::any Visitor::visitFrom_clause(codegen::PostgreSQLParser::From_clauseContext *ctx) {
  auto from_ident = ctx->from_list()
                        ->table_ref()[0]
                        ->relation_expr()
                        ->qualified_name()
                        ->colid()
                        ->identifier()
                        ->getText();

  return Table{from_ident};
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
  // TODO: support group_clause, having_clause

  Operator result = Table{kEmptyTableName};

  if (ctx->from_clause()) {
    result = std::any_cast<Table>(visit(ctx->from_clause()));
  }

  if (ctx->where_clause()) {
    auto filter_op = std::any_cast<Filter>(visit(ctx->where_clause()));
    filter_op.source = std::make_shared<Operator>(std::move(result));
    result = std::move(filter_op);
  }

  if (ctx->target_list_()) {
    auto target_list_opt = visit(ctx->target_list_());
    if (target_list_opt.has_value()) {
      auto projection_op = std::any_cast<Projection>(target_list_opt);
      projection_op.source = std::make_shared<Operator>(std::move(result));
      result = std::move(projection_op);
    }
  }

  return result;
}

std::any Visitor::visitWhere_clause(codegen::PostgreSQLParser::Where_clauseContext *ctx) {
  auto where_expr = std::any_cast<Expression>(visit(ctx->a_expr()));
  return Filter{where_expr, nullptr};
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

  // TODO: remaining clauses

  return select_clause;
}

}  // namespace stewkk::sql
