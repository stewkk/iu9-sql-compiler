#include <gmock/gmock.h>

#include <sstream>

#include <stewkk/sql/logic/parser/parser.hpp>

using ::testing::Eq;
using ::testing::VariantWith;

namespace stewkk::sql {

TEST(ParserTest, SelectAllFromSingleTable) {
    std::stringstream s{"SELECT * FROM users;"};

    Operator got = GetAST(s);

    ASSERT_THAT(got, VariantWith<Table>(Table{"users"}));
}

TEST(ParserTest, SelectSingleColumnFromSingleTable) {
    std::stringstream s{"SELECT id FROM users;"};

    Operator got = GetAST(s);

    ASSERT_THAT(got,
                VariantWith<Projection>(Projection{std::vector<std::string>{"id"},
                                                   std::make_shared<Operator>(Table{"users"})}));
}

TEST(ParserTest, SelectMultipleColumnsFromSingleTable) {
    std::stringstream s{"SELECT id, email, phone FROM users;"};

    Operator got = GetAST(s);

    ASSERT_THAT(got,
                VariantWith<Projection>(Projection{std::vector<std::string>{"id", "email", "phone"},
                                                   std::make_shared<Operator>(Table{"users"})}));
}

}  // namespace stewkk::sql
