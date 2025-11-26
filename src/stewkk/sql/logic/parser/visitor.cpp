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

std::any Visitor::visitChildren(antlr4::tree::ParseTree *node) {
  return codegen::PostgreSQLParserBaseVisitor::visitChildren(node);
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
  return visitStmtblock(ctx->stmtblock());
}

std::any Visitor::visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) {
  auto stmt = ctx->stmt();
  if (stmt.empty()) {
    return {};
  }
  return visitStmt(ctx->stmt()[0]);
}

std::any Visitor::visitSelectstmt(codegen::PostgreSQLParser::SelectstmtContext* ctx) {
  auto target_list = ctx->select_no_parens()
                    ->select_clause()
                    ->simple_select_intersect()[0]
                    ->simple_select_pramary()[0]
                    ->target_list_()
                    ->target_list();
  std::vector<Attribute> targets;
  if (!(target_list->target_el().size() == 1 && target_list->target_el(0)->getText() == "*")) {
    targets = target_list->target_el() | std::ranges::views::transform([this] (const auto& target_node) {
      auto expr_any = visit(target_node);
      if (Attribute* attr = std::any_cast<Attribute>(&expr_any)) {
        return *attr;
      }
      std::unreachable();
    }) | std::ranges::to<std::vector>();
  }

  auto from_ident = ctx->select_no_parens()
                        ->select_clause()
                        ->simple_select_intersect()[0]
                        ->simple_select_pramary()[0]
                        ->from_clause()
                        ->from_list()
                        ->table_ref()[0]
                        ->relation_expr()
                        ->qualified_name()
                        ->colid()
                        ->identifier()
                        ->getText();

  auto where_clause = ctx->select_no_parens()
                          ->select_clause()
                          ->simple_select_intersect()[0]
                          ->simple_select_pramary()[0]
                          ->where_clause();

  Operator result{Table{from_ident}};

  if (where_clause) {
    auto where_expr_any = visit(where_clause->a_expr());
    if (Expression* where_expr = std::any_cast<Expression>(&where_expr_any)) {
      result = Filter{*where_expr, std::make_shared<Operator>(result)};
    }
  }

  if (!targets.empty()) {
    result = Projection{targets, std::make_shared<Operator>(result)};
  }

  return result;
}

}  // namespace stewkk::sql
