#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

using ::testing::Eq;
using ::testing::IsTrue;
using ::testing::IsFalse;
using ::testing::Optional;
using ::testing::VariantWith;

namespace stewkk::sql {

const static std::string kProjectDir = std::getenv("PWD");

namespace {

std::string ReadFromFile(std::filesystem::path path) {
  std::ifstream f{path};
  std::ostringstream stream;
  stream << f.rdbuf();
  return stream.str();
}

} // namespace


TEST(ParserTest, SelectAllFromSingleTable) {
  std::stringstream s{"SELECT * FROM users;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Table>(Table{"users"}));
}

TEST(ParserTest, SelectSingleColumnFromSingleTable) {
  std::stringstream s{"SELECT users.id FROM users;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{std::vector<Expression>{{Attribute{"users", "id"}}},
                                                      std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectMultipleColumnsFromSingleTable) {
  std::stringstream s{"SELECT users.id, users.email, users.phone FROM users;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got,
              VariantWith<Projection>(Projection{
                  std::vector<Expression>{Attribute{"users", "id"}, Attribute{"users", "email"}, Attribute{"users", "phone"}},
                  std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectWithWhereClause) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age > 18;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"users", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{BinaryExpression{
                                      std::make_shared<Expression>(Attribute{"users", "age"}),
                                      BinaryOp::kGt, std::make_shared<Expression>(IntConst{18})}},
                                  std::make_shared<Operator>(Table{"users"})})}));
}

TEST(ParserTest, SelectWithTableAlias) {
  std::stringstream s{"SELECT u.id FROM users u WHERE u.age > 18;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"u", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{BinaryExpression{
                                      std::make_shared<Expression>(Attribute{"u", "age"}),
                                      BinaryOp::kGt, std::make_shared<Expression>(IntConst{18})}},
                                  std::make_shared<Operator>(Table{"users", "u"})})}));
}

TEST(ParserTest, SelectWithTableAsAlias) {
  std::stringstream s{"SELECT u.id FROM users AS u;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"u", "id"}},
                       std::make_shared<Operator>(Table{"users", "u"})}));
}

TEST(ParserTest, AliasColumnListRejected) {
  std::stringstream s{"SELECT u.id FROM users AS u(id);"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, SelectWithStringLiteral) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.name = 'Bob''s Market';"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"users", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{BinaryExpression{
                                      std::make_shared<Expression>(Attribute{"users", "name"}),
                                      BinaryOp::kEq,
                                      std::make_shared<Expression>(StringConst{"Bob's Market"})}},
                                  std::make_shared<Operator>(Table{"users"})})}));
}

TEST(ParserTest, SelectWithBetween) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age BETWEEN 18 AND 30;"};

  Operator got = GetAST(s).value().op;

  auto ge = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "age"}),
      BinaryOp::kGe,
      std::make_shared<Expression>(IntConst{18}),
  };
  auto le = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "age"}),
      BinaryOp::kLe,
      std::make_shared<Expression>(IntConst{30}),
  };
  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"users", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{BinaryExpression{
                                      std::make_shared<Expression>(std::move(ge)),
                                      BinaryOp::kAnd,
                                      std::make_shared<Expression>(std::move(le))}},
                                  std::make_shared<Operator>(Table{"users"})})}));
}

TEST(ParserTest, SelectWithNotBetween) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age NOT BETWEEN 18 AND 30;"};

  Operator got = GetAST(s).value().op;

  auto ge = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "age"}),
      BinaryOp::kGe,
      std::make_shared<Expression>(IntConst{18}),
  };
  auto le = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "age"}),
      BinaryOp::kLe,
      std::make_shared<Expression>(IntConst{30}),
  };
  auto between = BinaryExpression{
      std::make_shared<Expression>(std::move(ge)),
      BinaryOp::kAnd,
      std::make_shared<Expression>(std::move(le)),
  };
  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"users", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{UnaryExpression{
                                      UnaryOp::kNot,
                                      std::make_shared<Expression>(std::move(between))}},
                                  std::make_shared<Operator>(Table{"users"})})}));
}

TEST(ParserTest, SelectWithInList) {
  std::stringstream s{"SELECT m.id FROM markets AS m WHERE m.region IN ('AMERICA', 'ASIA');"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"m", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{InExpression{
                                      std::make_shared<Expression>(Attribute{"m", "region"}),
                                      {StringConst{"AMERICA"}, StringConst{"ASIA"}},
                                      false}},
                                  std::make_shared<Operator>(Table{"markets", "m"})})}));
}

TEST(ParserTest, SelectWithNotInList) {
  std::stringstream s{"SELECT m.id FROM markets AS m WHERE m.region NOT IN ('AMERICA', 'ASIA');"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"m", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{InExpression{
                                      std::make_shared<Expression>(Attribute{"m", "region"}),
                                      {StringConst{"AMERICA"}, StringConst{"ASIA"}},
                                      true}},
                                  std::make_shared<Operator>(Table{"markets", "m"})})}));
}

TEST(ParserTest, SelectWithBetweenSymmetricRejected) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age BETWEEN SYMMETRIC 18 AND 30;"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, SelectWithInSubqueryRejected) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age IN (SELECT users.age FROM users);"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, GetDotRepresentation) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age > 18;"};
  Operator op = GetAST(s).value().op;
  auto expected = ReadFromFile(kProjectDir + "/test/static/parser/expected.dot");

  auto got = GetDotRepresentation(op);

  ASSERT_THAT(got, Eq(expected));
}

TEST(ParserTest, SelectWithBooleanExpression) {
  std::stringstream s{"SELECT TRUE AND NULL OR FALSE AND NOT NULL;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       {BinaryExpression{
                           std::make_shared<Expression>(BinaryExpression{
                               std::make_shared<Expression>(Literal::kTrue),
                               BinaryOp::kAnd,
                               std::make_shared<Expression>(Literal::kNull),
                           }),
                           BinaryOp::kOr,
                           std::make_shared<Expression>(BinaryExpression{
                               std::make_shared<Expression>(Literal::kFalse),
                               BinaryOp::kAnd,
                               std::make_shared<Expression>(UnaryExpression{
                                   UnaryOp::kNot, std::make_shared<Expression>(Literal::kNull)}),
                           }),
                       }},
                       std::make_shared<Operator>(Table{"_EMPTY_TABLE_"})}));
}

TEST(ParserTest, SelectWithArithmeticalOperations) {
  std::stringstream s{"SELECT 1+2-3;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(
      got, VariantWith<Projection>(Projection{{BinaryExpression{
                                                  std::make_shared<Expression>(BinaryExpression{
                                                      std::make_shared<Expression>(IntConst{1}),
                                                      BinaryOp::kPlus,
                                                      std::make_shared<Expression>(IntConst{2}),
                                                  }),
                                                  BinaryOp::kMinus,
                                                  std::make_shared<Expression>(IntConst{3}),
                                              }},
                                              std::make_shared<Operator>(Table{"_EMPTY_TABLE_"})}));
}

TEST(ParserTest, GetDotRepresentationOfArithmeticalExpression) {
  std::stringstream s{"SELECT 1+2-3+4+5-6;"};

  Operator op = GetAST(s).value().op;
  auto expected = ReadFromFile(kProjectDir + "/test/static/parser/expected_arithmetical.dot");

  auto got = GetDotRepresentation(op);

  ASSERT_THAT(got, Eq(expected));
}

TEST(ParserTest, SyntaxError) {
  std::stringstream s{"xxx;"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.What(), Eq("syntax error"));
  ASSERT_THAT(got.Wraps(ErrorType::kSyntaxError), IsTrue());
}

TEST(ParserTest, NotSupportedError) {
  std::stringstream s{"INSERT INTO users (id) VALUES (1);"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.What(), Eq("insertstmt is currently unsupported"));
  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, EmptyQuery) {
  std::stringstream s{""};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Table>(Table{"_EMPTY_TABLE_"}));
}

TEST(ParserTest, SelectWithParens) {
  std::stringstream s{"((SELECT * FROM users));"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Table>(Table{"users"}));
}

TEST(ParserTest, EmptySelect) {
  std::stringstream s{"SELECT;"};

  Operator got = GetAST(s).value().op;

  ASSERT_THAT(got, VariantWith<Table>(Table{"_EMPTY_TABLE_"}));
}

TEST(ParserTest, SelectWithJoinDot) {
  std::stringstream s{"SELECT * FROM users, books;"};
  auto expected = ReadFromFile(kProjectDir + "/test/static/parser/expected_join.dot");
  Operator op = GetAST(s).value().op;

  auto got = GetDotRepresentation(op);

  ASSERT_THAT(got, Eq(expected));
}

TEST(ParserTest, SelectWithOuterJoinDot) {
  std::stringstream s{"SELECT * FROM users LEFT OUTER JOIN books ON users.book = books.id;"};
  auto expected = ReadFromFile(kProjectDir + "/test/static/parser/expected_outer_join.dot");
  Operator op = GetAST(s).value().op;

  auto got = GetDotRepresentation(op);

  ASSERT_THAT(got, Eq(expected));
}

TEST(ParserTest, OrderByAscDefault) {
  std::stringstream s{"SELECT * FROM users ORDER BY users.id;"};

  auto parsed = GetAST(s).value();

  ASSERT_THAT(parsed.op, VariantWith<Table>(Table{"users"}));
  ASSERT_THAT(parsed.required_order, Optional(SortOrder{{SortKey{"users", "id", Direction::kAsc}}}));
}

TEST(ParserTest, OrderByDesc) {
  std::stringstream s{"SELECT * FROM users ORDER BY users.id DESC;"};

  auto parsed = GetAST(s).value();

  ASSERT_THAT(parsed.required_order, Optional(SortOrder{{SortKey{"users", "id", Direction::kDesc}}}));
}

TEST(ParserTest, OrderByMultiKey) {
  std::stringstream s{"SELECT * FROM users ORDER BY users.id ASC, users.age DESC;"};

  auto parsed = GetAST(s).value();

  ASSERT_THAT(parsed.required_order, Optional(SortOrder{{
      SortKey{"users", "id", Direction::kAsc},
      SortKey{"users", "age", Direction::kDesc},
  }}));
}

TEST(ParserTest, OrderByNoOrderByClause) {
  std::stringstream s{"SELECT * FROM users;"};

  auto parsed = GetAST(s).value();

  ASSERT_THAT(parsed.required_order.has_value(), IsFalse());
}

TEST(ParserTest, OrderByIntOrdinalRejected) {
  std::stringstream s{"SELECT * FROM users ORDER BY 1;"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, OrderByUnqualifiedColumnRejected) {
  std::stringstream s{"SELECT * FROM users ORDER BY id;"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, OrderByNullsFirstRejected) {
  std::stringstream s{"SELECT * FROM users ORDER BY users.id NULLS FIRST;"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, SelectWithSumAggregateAlias) {
  std::stringstream s{
      "SELECT SUM(lineorder.lo_extendedprice * lineorder.lo_discount) AS revenue "
      "FROM lineorder;"};

  Operator got = GetAST(s).value().op;

  Expression revenue_arg = BinaryExpression{
      std::make_shared<Expression>(Attribute{"lineorder", "lo_extendedprice"}),
      BinaryOp::kMul,
      std::make_shared<Expression>(Attribute{"lineorder", "lo_discount"}),
  };
  Expression revenue = AggregateExpression{
      AggregateFunction::kSum,
      std::make_shared<Expression>(revenue_arg),
      false,
  };

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"", "__agg0"}},
                       std::make_shared<Operator>(Aggregation{
                           {},
                           std::vector<Expression>{revenue},
                           std::make_shared<Operator>(Table{"lineorder"}),
                       }),
                       std::vector<std::optional<std::string>>{"revenue"},
                   }));
}

TEST(ParserTest, SelectWithGroupByAndAggregate) {
  std::stringstream s{
      "SELECT date.d_year, SUM(lineorder.lo_revenue) AS revenue "
      "FROM lineorder JOIN date ON lineorder.lo_orderdate = date.d_datekey "
      "GROUP BY date.d_year;"};

  Operator got = GetAST(s).value().op;

  Expression year = Attribute{"date", "d_year"};
  Expression revenue = AggregateExpression{
      AggregateFunction::kSum,
      std::make_shared<Expression>(Attribute{"lineorder", "lo_revenue"}),
      false,
  };
  Expression join_qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"lineorder", "lo_orderdate"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"date", "d_datekey"}),
  };

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{year, Attribute{"", "__agg0"}},
                       std::make_shared<Operator>(Aggregation{
                           std::vector<Expression>{year},
                           std::vector<Expression>{revenue},
                           std::make_shared<Operator>(Join{
                               JoinType::kInner,
                               join_qual,
                               std::make_shared<Operator>(Table{"lineorder"}),
                               std::make_shared<Operator>(Table{"date"}),
                           }),
                       }),
                       std::vector<std::optional<std::string>>{std::nullopt, "revenue"},
                   }));
}

TEST(ParserTest, SelectWithCountStar) {
  std::stringstream s{"SELECT COUNT(*) FROM lineorder;"};

  Operator got = GetAST(s).value().op;

  Expression count_star = AggregateExpression{
      AggregateFunction::kCount,
      nullptr,
      true,
  };
  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"", "__agg0"}},
                       std::make_shared<Operator>(Aggregation{
                           {},
                           std::vector<Expression>{count_star},
                           std::make_shared<Operator>(Table{"lineorder"}),
                       }),
                   }));
}

TEST(ParserTest, UnsupportedAggregateModifiersRejected) {
  for (const char* sql : {
           "SELECT SUM(DISTINCT lineorder.lo_revenue) FROM lineorder;",
           "SELECT SUM(ALL lineorder.lo_revenue) FROM lineorder;",
           "SELECT SUM(lineorder.lo_revenue ORDER BY lineorder.lo_revenue) FROM lineorder;",
           "SELECT SUM(lineorder.lo_revenue) FILTER (WHERE lineorder.lo_discount > 0) FROM lineorder;",
           "SELECT SUM(lineorder.lo_revenue) OVER () FROM lineorder;",
       }) {
    std::stringstream s{sql};

    auto got = GetAST(s).error();

    ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
  }
}

TEST(ParserTest, UnsupportedGroupByFormsRejected) {
  std::stringstream s{
      "SELECT date.d_year, SUM(lineorder.lo_revenue) FROM lineorder "
      "GROUP BY ROLLUP(date.d_year);"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

TEST(ParserTest, HavingRejected) {
  std::stringstream s{
      "SELECT SUM(lineorder.lo_revenue) FROM lineorder "
      "HAVING SUM(lineorder.lo_revenue) > 0;"};

  auto got = GetAST(s).error();

  ASSERT_THAT(got.Wraps(ErrorType::kQueryNotSupported), IsTrue());
}

}  // namespace stewkk::sql
