#pragma once

#include <stewkk/sql/logic/parser/codegen/PostgreSQLParserBaseVisitor.h>

namespace stewkk::sql {

class Visitor : public codegen::PostgreSQLParserBaseVisitor {
  public:
    explicit Visitor(codegen::PostgreSQLParser* parser);
    virtual std::any visit(antlr4::tree::ParseTree *tree) override;
    virtual std::any visitRoot(codegen::PostgreSQLParser::RootContext* ctx) override;
    virtual std::any visitStmt(codegen::PostgreSQLParser::StmtContext *ctx) override;
    virtual std::any visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) override;
    virtual std::any visitSelect_no_parens(codegen::PostgreSQLParser::Select_no_parensContext *ctx) override;
    virtual std::any visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) override;
    virtual std::any visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) override;
    virtual std::any visitSelect_clause(codegen::PostgreSQLParser::Select_clauseContext *ctx) override;
    virtual std::any visitSimple_select_intersect(codegen::PostgreSQLParser::Simple_select_intersectContext *ctx) override;
    virtual std::any visitSimple_select_pramary(codegen::PostgreSQLParser::Simple_select_pramaryContext *ctx) override;
    virtual std::any visitTarget_list(codegen::PostgreSQLParser::Target_listContext *ctx) override;
    virtual std::any visitFrom_clause(codegen::PostgreSQLParser::From_clauseContext *ctx) override;
    virtual std::any visitWhere_clause(codegen::PostgreSQLParser::Where_clauseContext *ctx) override;
    virtual std::any visitSelect_with_parens(codegen::PostgreSQLParser::Select_with_parensContext *ctx) override;

  private:
    codegen::PostgreSQLParser* parser_;
};


}  // namespace stewkk::sql
