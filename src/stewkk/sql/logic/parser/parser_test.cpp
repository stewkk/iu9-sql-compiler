#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include <stewkk/sql/logic/parser/parser.hpp>

using ::testing::Eq;
using ::testing::IsTrue;
using ::testing::VariantWith;

namespace stewkk::sql {

const static std::string kProjectDir = std::getenv("PWD");

std::string ReadFromFile(std::filesystem::path path) {
  std::ifstream f{path};
  std::ostringstream stream;
  stream << f.rdbuf();
  return stream.str();
}

TEST(ParserTest, SelectAllFromSingleTable) {
  std::stringstream s{"SELECT * FROM users;"};

  Operator got = GetAST(s).value();

  ASSERT_THAT(got, VariantWith<Table>(Table{"users"}));
}

TEST(ParserTest, SelectSingleColumnFromSingleTable) {
  std::stringstream s{"SELECT users.id FROM users;"};

  Operator got = GetAST(s).value();

  ASSERT_THAT(got, VariantWith<Projection>(Projection{std::vector<Expression>{{Attribute{"users", "id"}}},
                                                      std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectMultipleColumnsFromSingleTable) {
  std::stringstream s{"SELECT users.id, users.email, users.phone FROM users;"};

  Operator got = GetAST(s).value();

  ASSERT_THAT(got,
              VariantWith<Projection>(Projection{
                  std::vector<Expression>{Attribute{"users", "id"}, Attribute{"users", "email"}, Attribute{"users", "phone"}},
                  std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectWithWhereClause) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age > 18;"};

  Operator got = GetAST(s).value();

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Expression>{Attribute{"users", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{BinaryExpression{
                                      std::make_shared<Expression>(Attribute{"users", "age"}),
                                      BinaryOp::kGt, std::make_shared<Expression>(IntConst{18})}},
                                  std::make_shared<Operator>(Table{"users"})})}));
}

TEST(ParserTest, GetDotRepresentation) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age > 18;"};
  Operator op = GetAST(s).value();
  auto expected = ReadFromFile(kProjectDir + "/test/static/parser/expected.dot");

  auto got = GetDotRepresentation(op);

  ASSERT_THAT(got, Eq(expected));
}

TEST(ParserTest, DISABLED_SelectWithBooleanExpression) {
  std::stringstream s{"SELECT TRUE AND NULL OR FALSE AND NOT NULL;"};

  Operator got = GetAST(s).value();
}

TEST(ParserTest, SelectWithArithmeticalOperations) {
  std::stringstream s{"SELECT 1+2-3;"};

  Operator got = GetAST(s).value();

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

  Operator op = GetAST(s).value();
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

  Operator got = GetAST(s).value();

  ASSERT_THAT(got, VariantWith<Table>(Table{"_EMPTY_TABLE_"}));
}

TEST(ParserTest, SelectWithParens) {
  std::stringstream s{"((SELECT * FROM users));"};

  Operator got = GetAST(s).value();

  ASSERT_THAT(got, VariantWith<Table>(Table{"users"}));
}

TEST(ParserTest, EmptySelect) {
  std::stringstream s{"SELECT;"};

  Operator got = GetAST(s).value();

  ASSERT_THAT(got, VariantWith<Table>(Table{"_EMPTY_TABLE_"}));
}

/*
** ORDER BY
** full support of a_expr
** aggregations: SELECT kind, sum(len) AS total FROM films GROUP BY kind;
 */

}  // namespace stewkk::sql
