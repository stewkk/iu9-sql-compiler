#pragma once

#include <stewkk/sql/logic/parser/codegen/PostgreSQLParserBaseVisitor.h>

namespace stewkk::sql {

class Visitor : public codegen::PostgreSQLParserBaseVisitor {
  public:
    explicit Visitor(codegen::PostgreSQLParser* parser);
    virtual std::any visitRoot(codegen::PostgreSQLParser::RootContext* ctx) override;
    virtual std::any visitStmt(codegen::PostgreSQLParser::StmtContext *ctx) override;
    virtual std::any visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) override;
    virtual std::any visitSelectstmt(codegen::PostgreSQLParser::SelectstmtContext* ctx) override;
    virtual std::any visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) override;
    virtual std::any visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) override;
    virtual std::any visitChildren(antlr4::tree::ParseTree *node) override;

  private:
    codegen::PostgreSQLParser* parser_;
};


}  // namespace stewkk::sql
