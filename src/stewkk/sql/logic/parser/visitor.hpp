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
    virtual std::any visitTarget_label(codegen::PostgreSQLParser::Target_labelContext *ctx) override;
    virtual std::any visitTarget_star(codegen::PostgreSQLParser::Target_starContext *ctx) override;
    virtual std::any visitFrom_clause(codegen::PostgreSQLParser::From_clauseContext *ctx) override;
    virtual std::any visitWhere_clause(codegen::PostgreSQLParser::Where_clauseContext *ctx) override;
    virtual std::any visitSelect_with_parens(codegen::PostgreSQLParser::Select_with_parensContext *ctx) override;
    virtual std::any visitA_expr_qual(codegen::PostgreSQLParser::A_expr_qualContext *ctx) override;
    virtual std::any visitA_expr_lessless(codegen::PostgreSQLParser::A_expr_lesslessContext *ctx) override;
    virtual std::any visitA_expr_or(codegen::PostgreSQLParser::A_expr_orContext *ctx) override;
    virtual std::any visitA_expr_and(codegen::PostgreSQLParser::A_expr_andContext *ctx) override;
    virtual std::any visitA_expr_between(codegen::PostgreSQLParser::A_expr_betweenContext *ctx) override;
    virtual std::any visitA_expr_in(codegen::PostgreSQLParser::A_expr_inContext *ctx) override;
    virtual std::any visitA_expr_unary_not(codegen::PostgreSQLParser::A_expr_unary_notContext *ctx) override;
    virtual std::any visitA_expr_isnull(
        codegen::PostgreSQLParser::A_expr_isnullContext *ctx) override;
    virtual std::any visitA_expr_is_not(
        codegen::PostgreSQLParser::A_expr_is_notContext *ctx) override;
    virtual std::any visitA_expr_like(codegen::PostgreSQLParser::A_expr_likeContext *ctx) override;
    virtual std::any visitA_expr_qual_op(
        codegen::PostgreSQLParser::A_expr_qual_opContext *ctx) override;
    virtual std::any visitA_expr_unary_qualop(
        codegen::PostgreSQLParser::A_expr_unary_qualopContext *ctx) override;
    virtual std::any visitA_expr_add(codegen::PostgreSQLParser::A_expr_addContext *ctx) override;
    virtual std::any visitA_expr_mul(codegen::PostgreSQLParser::A_expr_mulContext *ctx) override;
    virtual std::any visitA_expr_caret(
        codegen::PostgreSQLParser::A_expr_caretContext *ctx) override;
    virtual std::any visitA_expr_unary_sign(
        codegen::PostgreSQLParser::A_expr_unary_signContext *ctx) override;
    virtual std::any visitA_expr_at_time_zone(
        codegen::PostgreSQLParser::A_expr_at_time_zoneContext *ctx) override;
    virtual std::any visitA_expr_collate(
        codegen::PostgreSQLParser::A_expr_collateContext *ctx) override;
    virtual std::any visitA_expr_typecast(
        codegen::PostgreSQLParser::A_expr_typecastContext *ctx) override;
    virtual std::any visitC_expr_exists(codegen::PostgreSQLParser::C_expr_existsContext *ctx) override;
    virtual std::any visitC_expr_expr(codegen::PostgreSQLParser::C_expr_exprContext *ctx) override;
    virtual std::any visitC_expr_case(codegen::PostgreSQLParser::C_expr_caseContext *ctx) override;
    virtual std::any visitAexprconst(codegen::PostgreSQLParser::AexprconstContext *ctx) override;
    virtual std::any visitIconst(codegen::PostgreSQLParser::IconstContext *ctx) override;
    virtual std::any visitFrom_list(codegen::PostgreSQLParser::From_listContext *ctx) override;
    virtual std::any visitTable_ref(codegen::PostgreSQLParser::Table_refContext *ctx) override;
    virtual std::any visitRelation_expr(codegen::PostgreSQLParser::Relation_exprContext *ctx) override;
    virtual std::any visitQualified_name(codegen::PostgreSQLParser::Qualified_nameContext *ctx) override;

  private:
    codegen::PostgreSQLParser* parser_;
};


}  // namespace stewkk::sql
