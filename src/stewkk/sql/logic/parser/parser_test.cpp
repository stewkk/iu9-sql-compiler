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

}  // namespace stewkk::sql
