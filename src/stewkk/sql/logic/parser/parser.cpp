#include <stewkk/sql/logic/parser/parser.hpp>

#include <ranges>

#include <antlr4-runtime.h>

#include <stewkk/sql/logic/parser/codegen/PostgreSQLLexer.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParser.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParserBaseVisitor.h>

namespace stewkk::sql {

namespace {

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

  auto table_operator = Operator{Table{from_ident}};
  if (targets == std::vector<std::string>{{"*"}}) {
    return table_operator;
  }
  return Operator{Projection{targets, std::make_shared<Operator>(table_operator)}};
}

} // namespace

bool Projection::operator==(const Projection& other) const {
  return attributes == other.attributes && *source == *other.source;
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

}  // namespace stewkk::sql
