#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

TEST(MemoTest, AddGroup) {
  Memo memo;
  EXPECT_EQ(memo.GroupCount(), 0);

  auto group = memo.AddGroup(logical::Table{"t"});
  EXPECT_EQ(memo.GroupCount(), 1);

  auto exprs = group->GetLogicalExprs();
  ASSERT_EQ(exprs.size(), 1);
  EXPECT_TRUE(std::holds_alternative<logical::Table>(exprs[0]->root_operator));
  EXPECT_EQ(std::get<logical::Table>(exprs[0]->root_operator).name, "t");
}

TEST(MemoTest, AddSameGroupSecondReturnsIt) {
  Memo memo;
  auto first = memo.AddGroup(logical::Table{"t"});
  auto second = memo.AddGroup(logical::Table{"t"});

  EXPECT_EQ(first, second);
  EXPECT_EQ(memo.GroupCount(), 1);
}

}  // namespace stewkk::sql
