#include <stewkk/sql/logic/parser/parser.hpp>

#include <ranges>

#include <antlr4-runtime.h>

#include <stewkk/sql/logic/parser/codegen/PostgreSQLLexer.h>
#include <stewkk/sql/logic/parser/codegen/PostgreSQLParser.h>

#include <stewkk/sql/logic/parser/visitor.hpp>

namespace stewkk::sql {

Result<Operator> GetAST(std::istream& in) {
  antlr4::ANTLRInputStream antlr_input(in);
  codegen::PostgreSQLLexer lexer(&antlr_input);

  antlr4::CommonTokenStream tokens(&lexer);
  tokens.fill();

  for (auto token : tokens.getTokens()) {
    std::cout << token->toString() << std::endl;
  }

  codegen::PostgreSQLParser parser(&tokens);
  antlr4::tree::ParseTree* tree = parser.root();

  if (parser.getNumberOfSyntaxErrors() != 0) {
    return MakeError<ErrorType::kSyntaxError>("syntax error");
  }

  std::cout << tree->toStringTree(&parser, true) << std::endl << std::endl;

  Visitor visitor(&parser);

  std::any res;
  try {
    res = visitor.visit(tree);
  } catch (const Error& ex) {
    return std::unexpected(ex);
  }

  if (Operator* op = std::any_cast<Operator>(&res)) {
    return std::move(*op);
  }

  return Table{kEmptyTableName};
}

std::string GetDotRepresentation(const Expression& expr) {
  struct DotFormatter {
    std::string operator()(const BinaryExpression& expr) {
      return std::format("{} {} {}", std::visit(DotFormatter{}, *expr.lhs), ToString(expr.binop),
                         std::visit(DotFormatter{}, *expr.rhs));
    }
    std::string operator()(const UnaryExpression& expr) {
      return std::format("({} {})",ToString(expr.op), std::visit(DotFormatter{}, *expr.child));
    }
    std::string operator()(const Attribute& expr) {
      return ToString(expr);
    }
    std::string operator()(const IntConst& expr) {
      return std::to_string(expr);
    }
    std::string operator()(const Literal& expr) {
      return ToString(expr);
    }
  };
  return std::visit(DotFormatter{}, expr);
}

std::string GetDotRepresentation(const Operator& op) {
  // FIXME: refactor to use osstream
    struct DotFormatter {
        std::pair<std::string, std::string> operator()(const Projection& op) {
          auto exprs = op.expressions
                       | std::views::transform([](const Expression& expr) { return ToString(expr); })
                       | std::views::join_with(',') | std::ranges::to<std::string>();
          auto node = std::format("π {}", exprs);
          auto [source_node, rest] = std::visit(DotFormatter{}, *op.source);
          return {node, std::format("\"{}\"\n\"{}\" -> \"{}\"\n{}", node, source_node, node, rest)};
        }
        std::pair<std::string, std::string> operator()(const Filter& op) {
          auto node = std::format("σ {}", GetDotRepresentation(op.expr));
          auto [source_node, rest] = std::visit(DotFormatter{}, *op.source);
          return {node, std::format("\"{}\"\n\"{}\" -> \"{}\"\n{}", node, source_node, node, rest)};
        }
        std::pair<std::string, std::string> operator()(const Table& op) {
          auto node = std::format("{}", op.name);
          return {node, std::format("\"{}\"", node)};
        }
        std::pair<std::string, std::string> operator()(const CrossJoin& op) {
          auto [lhs_node, lhs_rest] = std::visit(DotFormatter{}, *op.lhs);
          auto [rhs_node, rhs_rest] = std::visit(DotFormatter{}, *op.rhs);
          auto node = std::format("crossjoin_{}_{}", lhs_node, rhs_node);
          auto rest = std::format("\"{}\"[label=\"×\"]\n\"{}\" -> \"{}\"\n\"{}\" -> \"{}\"\n{}\n{}", node, lhs_node, node, rhs_node,
                                  node, lhs_rest, rhs_rest);
          return {node, rest};
        }
        std::pair<std::string, std::string> operator()(const Join& op) {
          auto [lhs_node, lhs_rest] = std::visit(DotFormatter{}, *op.lhs);
          auto [rhs_node, rhs_rest] = std::visit(DotFormatter{}, *op.rhs);
          auto join_type = ToString(op.type);
          auto qual_expr = GetDotRepresentation(op.qual);
          auto node = std::format("join_{}_{}", lhs_node, rhs_node);
          auto rest = std::format(
              "\"{}\"[label=\"{}\\nON {}\"]\n\"{}\" -> \"{}\"\n\"{}\" -> \"{}\"\n{}\n{}", node,
              join_type, qual_expr, lhs_node, node, rhs_node, node, lhs_rest, rhs_rest);
          return {node, rest};
        }
    };
    auto [_, code] = std::visit(DotFormatter{}, op);
    return std::format("digraph G {{ rankdir=BT; {} }}\n", code);
}

}  // namespace stewkk::sql
