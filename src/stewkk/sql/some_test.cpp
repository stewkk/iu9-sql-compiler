#include <gmock/gmock.h>

using ::testing::Eq;
using ::testing::Optional;

using std::string_literals::operator""s;
using std::string_view_literals::operator""sv;

namespace stewkk::sql {

TEST(ExampleTest, APlusB) {
  ASSERT_THAT(2*2, Eq(4));
}

}  // namespace stewkk::sql
