#include <iostream>

#include <antlr4-runtime.h>

#include <stewkk/sql/codegen/TLexer.h>
#include <stewkk/sql/codegen/TParser.h>
#include <stewkk/sql/codegen/TParserBaseVisitor.h>

#include <stewkk/sql/logic/llvm_ir_builder.hpp>

using namespace stewkk::sql::codegen;
using namespace stewkk::sql::logic;
using namespace antlr4;

int main() {
  std::ifstream f("example.txt");

  ANTLRInputStream input(f);
  TLexer lexer(&input);
  CommonTokenStream tokens(&lexer);

  tokens.fill();
  for (auto token : tokens.getTokens()) {
    std::cout << token->toString() << std::endl;
  }

  TParser parser(&tokens);
  tree::ParseTree* tree = parser.main();

  std::cout << tree->toStringTree(&parser) << std::endl << std::endl;

  Visitor visitor;
  visitor.visit(tree);

  auto ir = visitor.GetIr();
  ir->print(llvm::errs(), nullptr);

  return 0;
}
