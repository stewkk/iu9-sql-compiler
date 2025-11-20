#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include <stewkk/sql/logic/parser/parser.hpp>

using ::testing::Eq;
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

  Operator got = GetAST(s);

  ASSERT_THAT(got, VariantWith<Table>(Table{"users"}));
}

TEST(ParserTest, SelectSingleColumnFromSingleTable) {
  std::stringstream s{"SELECT users.id FROM users;"};

  Operator got = GetAST(s);

  ASSERT_THAT(got, VariantWith<Projection>(Projection{std::vector<Attribute>{{"users", "id"}},
                                                      std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectMultipleColumnsFromSingleTable) {
  std::stringstream s{"SELECT users.id, users.email, users.phone FROM users;"};

  Operator got = GetAST(s);

  ASSERT_THAT(got,
              VariantWith<Projection>(Projection{
                  std::vector<Attribute>{{"users", "id"}, {"users", "email"}, {"users", "phone"}},
                  std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectWithWhereClause) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age > 18;"};

  Operator got = GetAST(s);

  ASSERT_THAT(got, VariantWith<Projection>(Projection{
                       std::vector<Attribute>{{"users", "id"}},
                       std::make_shared<Operator>(
                           Filter{Expression{BinaryExpression{
                                      std::make_shared<Expression>(Attribute{"users", "age"}),
                                      BinaryOp::kGt, std::make_shared<Expression>(IntConst{18})}},
                                  std::make_shared<Operator>(Table{"users"})})}));
}

TEST(ParserTest, GetDotRepresentation) {
  std::stringstream s{"SELECT users.id FROM users WHERE users.age > 18;"};
  Operator op = GetAST(s);
  auto expected = ReadFromFile(kProjectDir + "/test/static/parser/expected.dot");

  auto got = GetDotRepresentation(op);

  ASSERT_THAT(got, Eq(expected));
}

}  // namespace stewkk::sql
