#include <stewkk/sql/logic/parser/visitor.hpp>

#include <ranges>

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/result/error.hpp>

namespace stewkk::sql {

namespace {

Operator GetOperatorWithChild(Operator&& op, Operator&& child) {
  struct ChildSetter {
    Operator operator() (Table&& parent) {
      std::unreachable();
    }
    Operator operator() (Projection&& parent) {
      parent.source = std::make_shared<Operator>(std::move(child));
      return parent;
    }
    Operator operator() (Filter&& filter) {
      filter.source = std::make_shared<Operator>(std::move(child));
      return filter;
    }
    Operator operator() (CrossJoin&& cross_join) {
      std::unreachable();
    }
    Operator operator() (Join&& cross_join) {
      std::unreachable();
    }

    Operator&& child;
  };
  return std::visit(ChildSetter{std::move(child)}, std::move(op));
}

} // namespace

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

std::any Visitor::visit(antlr4::tree::ParseTree *tree) {
  std::string rule_name;
  int indentation = 0;
  auto& rule_names = parser_->getRuleNames();
  if (antlr4::RuleContext *rule_context = dynamic_cast<antlr4::RuleContext *>(tree)) {
    size_t rule_index = rule_context->getRuleIndex();
    rule_name = rule_names[rule_index];
    indentation = rule_context->depth()-1;
  }
  std::string indentation_str(indentation*4, ' ');
#ifdef DEBUG
  std::clog << std::format("{}visiting rule: {}\n", indentation_str, rule_name);
#endif
  auto tmp = codegen::PostgreSQLParserBaseVisitor::visit(tree);
#ifdef DEBUG
  std::clog << std::format("{}exiting rule: {}\n", indentation_str, rule_name);
#endif
  return tmp;
}

std::any Visitor::visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) {
  // NOTE: column reference has form: database_name.schema_name.table_name.column_name
  auto table = ctx->colid()->getText();
  if (ctx->indirection()) {
    auto column = ctx->indirection()->indirection_el(0)->attr_name()->getText();
    return Expression{Attribute{std::move(table), std::move(column)}};
  }
  return Expression{Attribute{"", std::move(table)}};
}

std::any Visitor::visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) {
  if (ctx->subquery_Op()) {
    throw Error{ErrorType::kQueryNotSupported, "subquery_Op is not supported"};
  }
  auto exprs = ctx->a_expr_like();
  if (exprs.size() == 1) {
    return visit(exprs.front());
  }
  auto lhs_expr = std::any_cast<Expression>(visit(exprs.front()));
  auto rhs_expr = std::any_cast<Expression>(visit(exprs.back()));
  BinaryOp op;
  if (ctx->LT()) {
    op = BinaryOp::kLt;
  } else if (ctx->GT()) {
    op = BinaryOp::kGt;
  } else if (ctx->EQUAL()) {
    op = BinaryOp::kEq;
  } else if (ctx->LESS_EQUALS()) {
    op = BinaryOp::kLe;
  } else if (ctx->GREATER_EQUALS()) {
    op = BinaryOp::kGe;
  } else if (ctx->NOT_EQUALS()) {
    op = BinaryOp::kNotEq;
  }
  return Expression{
    BinaryExpression {
      std::make_shared<Expression>(lhs_expr),
      op,
      std::make_shared<Expression>(rhs_expr),
    },
  };
}

Visitor::Visitor(codegen::PostgreSQLParser* parser) : parser_(parser) {}

std::any Visitor::visitRoot(codegen::PostgreSQLParser::RootContext *ctx) {
  return visit(ctx->stmtblock());
}

std::any Visitor::visitStmtmulti(codegen::PostgreSQLParser::StmtmultiContext* ctx) {
  auto stmt = ctx->stmt();
  if (stmt.empty()) {
    return {};
  }
  return visit(ctx->stmt()[0]);
}

std::any Visitor::visitSimple_select_intersect(codegen::PostgreSQLParser::Simple_select_intersectContext *ctx) {
  // TODO: INTERSECT (with ALL or DISTINCT)
  return visit(ctx->simple_select_pramary().front());
}

std::any Visitor::visitTarget_label(codegen::PostgreSQLParser::Target_labelContext *ctx) {
  // TODO: AS (rename operation)
  return visit(ctx->a_expr());
}

std::any Visitor::visitTarget_star(codegen::PostgreSQLParser::Target_starContext* ctx) {
  return {};
}

std::any Visitor::visitTarget_list(codegen::PostgreSQLParser::Target_listContext* ctx) {
  const auto& targets = ctx->target_el();
  auto target_expressions
      = targets | std::views::transform([this](const auto& target) { return visit(target); })
        | std::views::filter(
            [](const std::any& expr) { return expr.has_value(); })
        | std::views::transform(
            [](std::any expr) { return std::any_cast<Expression>(expr); })
        | std::ranges::to<std::vector>();

  if (!target_expressions.empty()) {
    return Operator{Projection{std::move(target_expressions), nullptr}};
  }

  return {};
}

std::any Visitor::visitFrom_clause(codegen::PostgreSQLParser::From_clauseContext *ctx) {
  return visit(ctx->from_list());
}

std::any Visitor::visitFrom_list(codegen::PostgreSQLParser::From_listContext *ctx) {
  auto table_ref = ctx->table_ref();
  auto result = std::any_cast<Operator>(visit(table_ref.front()));
  for (auto it = table_ref.begin()+1; it != table_ref.end(); it++) {
    auto rhs = std::any_cast<Operator>(visit(*it));
    result = CrossJoin{
        std::make_shared<Operator>(std::move(result)),
        std::make_shared<Operator>(std::move(rhs)),
    };
  }
  return result;
}

std::any Visitor::visitTable_ref(codegen::PostgreSQLParser::Table_refContext *ctx) {
  if (ctx->xmltable()) {
    throw Error{ErrorType::kQueryNotSupported, "xmltable is not supported"};
  }
  if (ctx->func_table()) {
    // NOTE: may want to support
    throw Error{ErrorType::kQueryNotSupported, "func_table is not supported"};
  }
  if (ctx->LATERAL_P()) {
    throw Error{ErrorType::kQueryNotSupported, "LATERAL clause is not supported"};
  }

  auto children = ctx->children;
  auto children_it = children.begin();

  Operator res;
  if (ctx->relation_expr()) {
    auto table = std::any_cast<std::string>(visit(ctx->relation_expr()));
    children_it++;
    res = Table{std::move(table)};
  }
  if (ctx->select_with_parens()) {
    res = std::any_cast<Operator>(visit(ctx->select_with_parens()));
    children_it++;
    children_it++;
  }
  auto table_refs = ctx->table_ref();
  auto table_ref_it = table_refs.begin();
  if (ctx->OPEN_PAREN()) {
    res = std::any_cast<Operator>(visit(*table_ref_it));
    children_it++;
    table_ref_it++;
  }

  if (ctx->alias_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "alias_clause is not supported"};
  }
  if (ctx->tablesample_clause()) {
    throw Error{ErrorType::kQueryNotSupported, "tablesample_clause is not supported"};
  }

  for (; children_it != children.end(); children_it++) {
    auto text = (*children_it)->getText();
    if (text == "CROSS") {
      // CROSS JOIN table_ref
      children_it += 2;
      auto rhs = std::any_cast<Operator>(visit(*children_it));
      res = CrossJoin{
          std::make_shared<Operator>(std::move(res)),
          std::make_shared<Operator>(std::move(rhs)),
      };
    } else if (text == "NATURAL") {
      throw Error{ErrorType::kQueryNotSupported, "NATURAL clause is not supported"};
    } else {
      Operator rhs;
      auto join_type = JoinType::kInner;
      Expression qual_expression;
      if (text == "JOIN") {
        // JOIN table_ref join_qual
        children_it++;
        rhs = std::any_cast<Operator>(visit(*children_it));
        children_it++;
        qual_expression = std::any_cast<Expression>(visit(*children_it));
      } else {
        // join_type JOIN table_ref join_qual
        join_type = std::any_cast<JoinType>(visit(*children_it));
        children_it += 2;
        rhs = std::any_cast<Operator>(visit(*children_it));
        children_it++;
        qual_expression = std::any_cast<Expression>(visit(*children_it));
      }
      res = Join{
          join_type,
          std::move(qual_expression),
          std::make_shared<Operator>(std::move(res)),
          std::make_shared<Operator>(std::move(rhs)),
      };
    }
  }
  return res;
}

std::any Visitor::visitJoin_type(codegen::PostgreSQLParser::Join_typeContext *ctx) {
  if (ctx->INNER_P()) {
    return JoinType::kInner;
  }
  if (ctx->FULL()) {
    return JoinType::kFull;
  }
  if (ctx->LEFT()) {
    return JoinType::kLeft;
  }
  return JoinType::kRight;
}

std::any Visitor::visitJoin_qual(codegen::PostgreSQLParser::Join_qualContext *ctx) {
  if (ctx->USING()) {
    throw Error{ErrorType::kQueryNotSupported, "USING clause is not supported"};
  }
  return visit(ctx->a_expr());
}

std::any Visitor::visitRelation_expr(codegen::PostgreSQLParser::Relation_exprContext *ctx) {
  if (ctx->ONLY()) {
    throw Error{ErrorType::kQueryNotSupported, "ONLY clause is not supported"};
  }
  if (ctx->STAR()) {
    throw Error{ErrorType::kQueryNotSupported, "tablename * clause is not supported"};
  }
  return visit(ctx->qualified_name());
}

std::any Visitor::visitQualified_name(codegen::PostgreSQLParser::Qualified_nameContext *ctx) {
  if (ctx->indirection()) {
    throw Error{ErrorType::kQueryNotSupported, "bare column names are not supported"};
  }
  return ctx->colid()->getText();
}

std::any Visitor::visitSimple_select_pramary(
    codegen::PostgreSQLParser::Simple_select_pramaryContext *ctx) {
  if (ctx->select_with_parens()) {
    return visit(ctx->select_with_parens());
  }
  if (!ctx->SELECT()) {
    throw Error{ErrorType::kQueryNotSupported, "VALUES and TABLE clauses are not supported"};
  }
  if (ctx->distinct_clause()) {
    // TODO: support distinct clause
    throw Error{ErrorType::kQueryNotSupported, "DISTINCT clause is not supported"};
  }

  // NOTE: all_clause_ is ignored
  // TODO: support group_clause, having_clause

  Operator result = Table{kEmptyTableName};

  if (ctx->from_clause()) {
    result = std::any_cast<Operator>(visit(ctx->from_clause()));
  }

  if (ctx->where_clause()) {
    auto filter = std::any_cast<Operator>(visit(ctx->where_clause()));
    result = GetOperatorWithChild(std::move(filter), std::move(result));
  }

  if (ctx->target_list_()) {
    auto target_list_opt = visit(ctx->target_list_());
    if (target_list_opt.has_value()) {
      auto projection = std::any_cast<Operator>(target_list_opt);
      result = GetOperatorWithChild(std::move(projection), std::move(result));
    }
  }

  return result;
}

std::any Visitor::visitWhere_clause(codegen::PostgreSQLParser::Where_clauseContext *ctx) {
  auto where_expr = std::any_cast<Expression>(visit(ctx->a_expr()));
  return Operator{Filter{where_expr, nullptr}};
}

std::any Visitor::visitSelect_clause(codegen::PostgreSQLParser::Select_clauseContext *ctx) {
  // TODO: UNION and EXCEPT (with ALL or DISTINCT)
  return visit(ctx->simple_select_intersect().front());
}

std::any Visitor::visitSelect_with_parens(codegen::PostgreSQLParser::Select_with_parensContext *ctx) {
  if (ctx->select_with_parens()) {
    return visit(ctx->select_with_parens());
  }
  return visit(ctx->select_no_parens());
}

std::any Visitor::visitSelect_no_parens(codegen::PostgreSQLParser::Select_no_parensContext *ctx) {
  if (ctx->with_clause()) {
    // TODO
  }

  auto select_clause = std::any_cast<Operator>(visit(ctx->select_clause()));

  // TODO: remaining clauses

  return select_clause;
}

std::any Visitor::visitA_expr_qual(codegen::PostgreSQLParser::A_expr_qualContext *ctx) {
  if (ctx->qual_op()) {
    throw Error{ErrorType::kQueryNotSupported, "qualified operators are not supported"};
  }
  return visit(ctx->a_expr_lessless());
}

std::any Visitor::visitA_expr_lessless(codegen::PostgreSQLParser::A_expr_lesslessContext *ctx) {
  if (ctx->a_expr_or().size() > 1) {
    throw Error{ErrorType::kQueryNotSupported, "<< and >> operators are not supported"};
  }
  return visit(ctx->a_expr_or(0));
}

std::any Visitor::visitA_expr_or(codegen::PostgreSQLParser::A_expr_orContext *ctx) {
  const auto& exprs = ctx->a_expr_and();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  for (size_t i = 1; i < exprs.size(); i++) {
    auto tmp = std::any_cast<Expression>(visit(exprs[i]));
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kOr,
                              std::make_shared<Expression>(std::move(tmp))};
  }
  return result;
}

std::any Visitor::visitA_expr_and(codegen::PostgreSQLParser::A_expr_andContext *ctx) {
  const auto& exprs = ctx->a_expr_between();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  for (size_t i = 1; i < exprs.size(); i++) {
    auto tmp = std::any_cast<Expression>(visit(exprs[i]));
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kAnd,
                              std::make_shared<Expression>(std::move(tmp))};
  }
  return result;
}

std::any Visitor::visitA_expr_between(codegen::PostgreSQLParser::A_expr_betweenContext *ctx) {
  if (ctx->BETWEEN()) {
    // NOTE: may want to support
    throw Error{ErrorType::kQueryNotSupported, "BETWEEN clause is not supported"};
  }
  return visit(ctx->a_expr_in(0));
}

std::any Visitor::visitA_expr_in(codegen::PostgreSQLParser::A_expr_inContext *ctx) {
  if (ctx->IN_P()) {
    // NOTE: may want to support
    throw Error{ErrorType::kQueryNotSupported, "IN clause is not supported"};
  }
  return visit(ctx->a_expr_unary_not());
}

std::any Visitor::visitA_expr_unary_not(codegen::PostgreSQLParser::A_expr_unary_notContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_isnull()));
  if (ctx->NOT()) {
    result = UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_isnull(codegen::PostgreSQLParser::A_expr_isnullContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_is_not()));
  if (ctx->ISNULL()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kNull)};
  }
  if (ctx->NOTNULL()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kNull)};
    result = UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_is_not(codegen::PostgreSQLParser::A_expr_is_notContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_compare()));
  if (!ctx->IS()) {
    return result;
  }

  if (ctx->NULL_P()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kNull)};
  } else if (ctx->TRUE_P()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kTrue)};
  } else if (ctx->FALSE_P()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kFalse)};
  } else if (ctx->UNKNOWN()) {
    result = BinaryExpression{std::make_shared<Expression>(std::move(result)), BinaryOp::kEq, std::make_shared<Expression>(Literal::kUnknown)};
  } else if (ctx->DISTINCT()) {
    throw Error{ErrorType::kQueryNotSupported, "IS DISTINCT FROM clause is not supported"};
  } else if (ctx->OF()) {
    throw Error{ErrorType::kQueryNotSupported, "IS OF (type_list) clause is not supported"};
  } else if (ctx->DOCUMENT_P()) {
    throw Error{ErrorType::kQueryNotSupported, "IS DOCUMENT clause is not supported"};
  } else if (ctx->NORMALIZED()) {
    throw Error{ErrorType::kQueryNotSupported, "IS NORMALIZED clause is not supported"};
  }

  if (ctx->NOT()) {
    result = UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_like(codegen::PostgreSQLParser::A_expr_likeContext *ctx) {
  const auto& exprs = ctx->a_expr_qual_op();
  if (exprs.size() > 1) {
    // NOTE: may want to implement
    throw Error{ErrorType::kQueryNotSupported, "LIKE clause is not supported"};
  }
  return visit(exprs.front());
}

std::any Visitor::visitA_expr_qual_op(codegen::PostgreSQLParser::A_expr_qual_opContext *ctx) {
  const auto& exprs = ctx->a_expr_unary_qualop();
  if (exprs.size() > 1) {
    // NOTE: may want to implement
    throw Error{ErrorType::kQueryNotSupported, "qual_op is not supported"};
  }
  return visit(exprs.front());
}

std::any Visitor::visitA_expr_unary_qualop(codegen::PostgreSQLParser::A_expr_unary_qualopContext *ctx) {
  if (ctx->qual_op()) {
    // NOTE: may want to implement
    throw Error{ErrorType::kQueryNotSupported, "unary_qual_op is not supported"};
  }
  return visit(ctx->a_expr_add());
}

std::any Visitor::visitA_expr_add(codegen::PostgreSQLParser::A_expr_addContext *ctx) {
  const auto& exprs = ctx->a_expr_mul();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  auto expr_it = exprs.cbegin()+1;

  const auto& children = ctx->children;
  auto op_it = children.cbegin()+1;

  while (expr_it != exprs.cend()) {
    auto op = (*op_it)->getText();
    auto rhs = std::any_cast<Expression>(visit(*expr_it));
    if (op == "+") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kPlus,
          std::make_shared<Expression>(std::move(rhs)),
      };
    } else if (op == "-") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kMinus,
          std::make_shared<Expression>(std::move(rhs)),
      };
    }
    op_it += 2;
    expr_it++;
  }
  return result;
}

std::any Visitor::visitA_expr_mul(codegen::PostgreSQLParser::A_expr_mulContext *ctx) {
  const auto& exprs = ctx->a_expr_caret();
  auto result = std::any_cast<Expression>(visit(exprs.front()));
  auto expr_it = exprs.cbegin()+1;

  const auto& children = ctx->children;
  auto op_it = children.cbegin()+1;

  while (expr_it != exprs.cend()) {
    auto op = (*op_it)->getText();
    auto rhs = std::any_cast<Expression>(visit(*expr_it));
    if (op == "*") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kMul,
          std::make_shared<Expression>(std::move(rhs)),
      };
    } else if (op == "/") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kDiv,
          std::make_shared<Expression>(std::move(rhs)),
      };
    } else if (op == "%") {
      result = BinaryExpression{
          std::make_shared<Expression>(std::move(result)),
          BinaryOp::kMod,
          std::make_shared<Expression>(std::move(rhs)),
      };
    }
    op_it += 2;
    expr_it++;
  }
  return result;
}

std::any Visitor::visitA_expr_caret(codegen::PostgreSQLParser::A_expr_caretContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_unary_sign(0)));
  if (ctx->CARET()) {
    auto rhs = std::any_cast<Expression>(visit(ctx->a_expr_unary_sign(1)));
    result = BinaryExpression{
        std::make_shared<Expression>(std::move(result)),
        BinaryOp::kPow,
        std::make_shared<Expression>(std::move(rhs)),
    };
  }
  return result;
}

std::any Visitor::visitA_expr_unary_sign(codegen::PostgreSQLParser::A_expr_unary_signContext *ctx) {
  auto result = std::any_cast<Expression>(visit(ctx->a_expr_at_time_zone()));
  if (ctx->MINUS()) {
    result = UnaryExpression{UnaryOp::kMinus, std::make_shared<Expression>(std::move(result))};
  }
  return result;
}

std::any Visitor::visitA_expr_at_time_zone(codegen::PostgreSQLParser::A_expr_at_time_zoneContext *ctx) {
  if (ctx->AT()) {
    throw Error{ErrorType::kQueryNotSupported, "AT TIME ZONE literal is not supported"};
  }
  return visit(ctx->a_expr_collate());
}

std::any Visitor::visitA_expr_collate(codegen::PostgreSQLParser::A_expr_collateContext *ctx) {
  if (ctx->COLLATE()) {
    throw Error{ErrorType::kQueryNotSupported, "COLLATE clause is not supported"};
  }
  return visit(ctx->a_expr_typecast());
}

std::any Visitor::visitA_expr_typecast(codegen::PostgreSQLParser::A_expr_typecastContext *ctx) {
  if (!ctx->TYPECAST().empty()) {
    // NOTE: may want to support
    throw Error{ErrorType::kQueryNotSupported, "TYPECAST clause is not supported"};
  }
  return visit(ctx->c_expr());
}

std::any Visitor::visitC_expr_exists(codegen::PostgreSQLParser::C_expr_existsContext *ctx) {
  // NOTE: may want to support
  throw Error{ErrorType::kQueryNotSupported, "EXISTS clause is not supported"};
}

std::any Visitor::visitC_expr_expr(codegen::PostgreSQLParser::C_expr_exprContext *ctx) {
  if (ctx->columnref()) {
    return visit(ctx->columnref());
  }
  if (ctx->aexprconst()) {
    return visit(ctx->aexprconst());
  }
  // NOTE: may want to support
  throw Error{ErrorType::kQueryNotSupported, "c_expr is not fully supported"};
}

std::any Visitor::visitC_expr_case(codegen::PostgreSQLParser::C_expr_caseContext *ctx) {
  // NOTE: may want to support
  throw Error{ErrorType::kQueryNotSupported, "CASE clause is not supported"};
}

std::any Visitor::visitAexprconst(codegen::PostgreSQLParser::AexprconstContext *ctx) {
  if (ctx->constinterval()) {
    throw Error{ErrorType::kQueryNotSupported, "intervals are not supported"};
  }
  if (ctx->sconst()) {
    throw Error{ErrorType::kQueryNotSupported, "strings are not supported"};
  }
  if (ctx->xconst()) {
    throw Error{ErrorType::kQueryNotSupported, "hex literals are not supported"};
  }
  if (ctx->bconst()) {
    throw Error{ErrorType::kQueryNotSupported, "binary literals are not supported"};
  }
  if (ctx->fconst()) {
    throw Error{ErrorType::kQueryNotSupported, "float literals are not supported"};
  }
  if (ctx->TRUE_P()) {
    return Expression{Literal::kTrue};
  }
  if (ctx->FALSE_P()) {
    return Expression{Literal::kFalse};
  }
  if (ctx->NULL_P()) {
    return Expression{Literal::kNull};
  }
  if (ctx->iconst()) {
    return visit(ctx->iconst());
  }
  throw Error{ErrorType::kUnknown, "some literal is not supported"};
}

std::any Visitor::visitIconst(codegen::PostgreSQLParser::IconstContext *ctx) {
  if (!ctx->Integral()) {
    throw Error{ErrorType::kQueryNotSupported, "binary, octal and hexadecimal constants are not supported"};
  }

  std::stringstream ss(ctx->Integral()->getText());
  int64_t value;

  if (ss >> value) {
    return Expression{IntConst{value}};
  } else {
    throw Error{ErrorType::kConversionError, std::format("integer literal \"{}\" cannot be converted to 64-bit signed integer", ctx->Integral()->getText())};
  }
}

}  // namespace stewkk::sql
