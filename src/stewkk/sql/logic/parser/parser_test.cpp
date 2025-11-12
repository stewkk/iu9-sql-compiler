#include <gmock/gmock.h>

#include <sstream>

#include <stewkk/sql/logic/parser/parser.hpp>

using ::testing::Eq;

namespace stewkk::sql {

TEST(ParserTest, APlusB) {
    std::stringstream s{"CREATE TABLE hobbies_r (\nname		text,\nperson 		text\n);"};

    GetAST(s);

    ASSERT_THAT(2, Eq(3));
}

}  // namespace stewkk::sql
