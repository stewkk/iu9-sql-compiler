#include <stewkk/sql/logic/parser/parser.hpp>

#include <ranges>

#include <antlr4-runtime.h>

#include <stewkk/sql/logic/parser/codegen/PostgreSQLLexer.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParser.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParserBaseVisitor.h>

namespace stewkk::sql {

namespace {

class ExprVisitor : public codegen::PostgreSQLParserBaseVisitor {
  virtual std::any visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) override;
};

std::any ExprVisitor::visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) {
  auto lhs = ctx->a_expr_like(0);
  auto rhs = ctx->a_expr_like(1);
  return Expression{
      BinaryExpression{std::make_shared<Expression>(Attribute{lhs->getText()}), BinaryOp::kGt,
                       std::make_shared<Expression>(IntConst{std::stoi(rhs->getText())})}};
}

class Visitor : public codegen::PostgreSQLParserBaseVisitor {
  public:
    explicit Visitor(codegen::PostgreSQLParser* parser);
    virtual std::any visitRoot(codegen::PostgreSQLParser::RootContext* ctx) override;
    virtual std::any visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) override;
    virtual std::any visitSelectstmt(codegen::PostgreSQLParser::SelectstmtContext* ctx) override;
    virtual std::any visitChildren(antlr4::tree::ParseTree *node) override;

  private:
    codegen::PostgreSQLParser* parser_;
};

std::any Visitor::visitChildren(antlr4::tree::ParseTree *node) {
  std::cout << std::format("visiting {}, number of children {}\n", node->toString(), node->children.size());
  return codegen::PostgreSQLParserBaseVisitor::visitChildren(node);
}

Visitor::Visitor(codegen::PostgreSQLParser* parser) : parser_(parser) {}

std::any Visitor::visitRoot(codegen::PostgreSQLParser::RootContext *ctx) {
  return visitStmtblock(ctx->stmtblock());
}

std::any Visitor::visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) {
  return visitStmt(ctx->stmt()[0]);
}

std::any Visitor::visitSelectstmt(codegen::PostgreSQLParser::SelectstmtContext* ctx) {
  std::cout << ctx->toStringTree(parser_, true) << std::endl;
  auto targets = ctx->select_no_parens()
                    ->select_clause()
                    ->simple_select_intersect()[0]
                    ->simple_select_pramary()[0]
                    ->target_list_()
                    ->target_list()
    ->target_el() | std::ranges::views::transform([] (const auto& target_node) {
      return target_node->getText();
    }) | std::ranges::to<std::vector>();

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
    ExprVisitor expr_visitor;
    auto where_expr_any = expr_visitor.visit(where_clause->a_expr());
    if (Expression* where_expr = std::any_cast<Expression>(&where_expr_any)) {
      result = Filter{*where_expr, std::make_shared<Operator>(result)};
    }
  }

  if (targets != std::vector<std::string>{{"*"}}) {
    result = Projection{targets, std::make_shared<Operator>(result)};
  }

  return result;
}

} // namespace

bool Projection::operator==(const Projection& other) const {
  return attributes == other.attributes && *source == *other.source;
}

bool Filter::operator==(const Filter& other) const {
  return expr == other.expr && *source == *other.source;
}

bool BinaryExpression::operator==(const BinaryExpression& other) const {
  return *lhs == *other.lhs && binop == other.binop && *rhs == *other.rhs;
}

Operator GetAST(std::istream& in) {
  antlr4::ANTLRInputStream antlr_input(in);
  codegen::PostgreSQLLexer lexer(&antlr_input);

  antlr4::CommonTokenStream tokens(&lexer);
  tokens.fill();

  for (auto token : tokens.getTokens()) {
    std::cout << token->toString() << std::endl;
  }

  codegen::PostgreSQLParser parser(&tokens);
  antlr4::tree::ParseTree* tree = parser.root();

  std::cout << tree->toStringTree(&parser, true) << std::endl << std::endl;

  Visitor visitor(&parser);
  auto res = visitor.visit(tree);

  if (Operator* op = std::any_cast<Operator>(&res)) {
    return std::move(*op);
  }

  return Table{"NOOP"};
}

std::string GetDotRepresentation(const Expression& expr) {
  struct DotFormatter {
    std::string operator()(const BinaryExpression& expr) {
      return std::format("{} {} {}", std::visit(DotFormatter{}, *expr.lhs), ToString(expr.binop),
                         std::visit(DotFormatter{}, *expr.rhs));
    }
    std::string operator()(const Attribute& expr) {
      return expr;
    }
    std::string operator()(const IntConst& expr) {
      return std::to_string(expr);
    }
  };
  return std::visit(DotFormatter{}, expr);
}

std::string GetDotRepresentation(const Operator& op) {
    struct DotFormatter {
        std::pair<std::string, std::string> operator()(const Projection& op) {
          auto attrs
              = op.attributes | std::ranges::views::join_with(',') | std::ranges::to<std::string>();
          auto node = std::format("\"π {}\"", attrs);
          auto [source_node, rest] = std::visit(DotFormatter{}, *op.source);
          return {node, std::format("{}\n{} -> {}\n{}", node, source_node, node, rest)};
        }
        std::pair<std::string, std::string> operator()(const Filter& op) {
          auto node = std::format("\"σ {}\"", GetDotRepresentation(op.expr));
          auto [source_node, rest] = std::visit(DotFormatter{}, *op.source);
          return {node, std::format("{}\n{} -> {}\n{}", node, source_node, node, rest)};
        }
        std::pair<std::string, std::string> operator()(const Table& op) {
          auto node = std::format("\"{}\"", op.name);
          return {node, node};
        }
    };
    auto [_, code] = std::visit(DotFormatter{}, op);
    return std::format("digraph G {{ rankdir=BT; {} }}\n", code);
}

std::string ToString(BinaryOp binop) {
    switch (binop) {
      case BinaryOp::kGt:
        return ">";
    }
}

}  // namespace stewkk::sql
