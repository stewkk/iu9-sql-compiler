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
    virtual std::any visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) override;
    virtual std::any visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) override;
    virtual std::any visitChildren(antlr4::tree::ParseTree *node) override;

  private:
    codegen::PostgreSQLParser* parser_;
};

std::any Visitor::visitChildren(antlr4::tree::ParseTree *node) {
  std::cout << std::format("visiting {}, number of children {}\n", node->toString(), node->children.size());
  return codegen::PostgreSQLParserBaseVisitor::visitChildren(node);
}

std::any Visitor::visitColumnref(codegen::PostgreSQLParser::ColumnrefContext *ctx) {
  auto table = ctx->colid()->getText();
  auto column = ctx->indirection()->indirection_el(0)->attr_name()->getText();
  std::cout << "visitColumnref " << table << ' ' << column << std::endl;
  return Attribute{std::move(table), std::move(column)};
}

std::any Visitor::visitA_expr_compare(codegen::PostgreSQLParser::A_expr_compareContext *ctx) {
  auto lhs = ctx->a_expr_like(0);
  auto rhs = ctx->a_expr_like(1);
  if (!lhs || !rhs) {
    return visitChildren(ctx);
  }
  auto lhs_any = visit(lhs);
  if (Attribute* attr = std::any_cast<Attribute>(&lhs_any)) {
    return Expression{
        BinaryExpression{std::make_shared<Expression>(*attr), BinaryOp::kGt,
                         std::make_shared<Expression>(IntConst{std::stoi(rhs->getText())})}};
  }
  std::unreachable();
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
  auto target_list = ctx->select_no_parens()
                    ->select_clause()
                    ->simple_select_intersect()[0]
                    ->simple_select_pramary()[0]
                    ->target_list_()
                    ->target_list();
  std::vector<Attribute> targets;
  if (!(target_list->target_el().size() == 1 && target_list->target_el(0)->getText() == "*")) {
    targets = target_list->target_el() | std::ranges::views::transform([this] (const auto& target_node) {
      auto expr_any = visit(target_node);
      if (Attribute* attr = std::any_cast<Attribute>(&expr_any)) {
        return *attr;
      }
      std::unreachable();
    }) | std::ranges::to<std::vector>();
  }

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
    auto where_expr_any = visit(where_clause->a_expr());
    if (Expression* where_expr = std::any_cast<Expression>(&where_expr_any)) {
      result = Filter{*where_expr, std::make_shared<Operator>(result)};
    }
  }

  if (!targets.empty()) {
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
      return ToString(expr);
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
          auto attrs = op.attributes
                       | std::views::transform([](const Attribute& attr) { return ToString(attr); })
                       | std::views::join_with(',') | std::ranges::to<std::string>();
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

std::string ToString(const Attribute& attr) {
  return std::format("{}.{}", attr.table, attr.name);
}

}  // namespace stewkk::sql
