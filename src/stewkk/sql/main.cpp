#include <iostream>

#include <antlr4-runtime.h>

#include <stewkk/sql/logic/parser/codegen/PostgreSQLLexer.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParser.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParserBaseVisitor.h>

using namespace stewkk::sql::codegen;
using namespace antlr4;

int main() {
  std::cout << "Hello\n";

  return 0;
}
