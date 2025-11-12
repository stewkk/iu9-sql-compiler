#include <stewkk/sql/logic/parser/parser.hpp>

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

  private:
    codegen::PostgreSQLParser* parser_;
};

Visitor::Visitor(codegen::PostgreSQLParser* parser) : parser_(parser) {}

std::any Visitor::visitRoot(codegen::PostgreSQLParser::RootContext *ctx) {
  return visitStmtblock(ctx->stmtblock());
}

std::any Visitor::visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) {
  return visitStmt(ctx->stmt()[0]);
}

std::any Visitor::visitSelectstmt(codegen::PostgreSQLParser::SelectstmtContext* ctx) {
  std::cout << ctx->toStringTree(parser_, true) << std::endl;
  auto target = ctx->select_no_parens()
                    ->select_clause()
                    ->simple_select_intersect()[0]
                    ->simple_select_pramary()[0]
                    ->target_list_()
                    ->target_list()
                    ->target_el()[0]
                    ->getText();
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

  std::cout << target << ' ' << from_ident << std::endl;

  return Operator{Table{from_ident}};
}

} // namespace

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
    return *op;
  }

  return Table{"NOOP"};
}

}  // namespace stewkk::sql
