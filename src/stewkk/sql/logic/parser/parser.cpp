#include <stewkk/sql/logic/parser/parser.hpp>

#include <antlr4-runtime.h>

#include <stewkk/sql/logic/parser/codegen/PostgreSQLLexer.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParser.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParserBaseVisitor.h>

namespace stewkk::sql {

void GetAST(std::istream& in) {
  antlr4::ANTLRInputStream antlr_input(in);
  codegen::PostgreSQLLexer lexer(&antlr_input);

  antlr4::CommonTokenStream tokens(&lexer);
  tokens.fill();

  for (auto token : tokens.getTokens()) {
    std::cout << token->toString() << std::endl;
  }

  codegen::PostgreSQLParser parser(&tokens);
  antlr4::tree::ParseTree* tree = parser.root();

  std::cout << tree->toStringTree(&parser) << std::endl << std::endl;
}

}  // namespace stewkk::sql
