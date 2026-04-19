#include <gmock/gmock.h>

#include <sstream>

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/parser/parser.hpp>

namespace stewkk::sql {

TEST(MemoTest, AddGroup) {
  Memo memo;
  EXPECT_EQ(memo.GroupCount(), 0);

  auto expr = memo.AddGroup(logical::Table{"t"});
  EXPECT_EQ(memo.GroupCount(), 1);

  auto exprs = expr->group->GetLogicalExprs();
  ASSERT_EQ(exprs.size(), 1);
  EXPECT_TRUE(std::holds_alternative<logical::Table>(exprs[0]->root_operator));
  EXPECT_EQ(std::get<logical::Table>(exprs[0]->root_operator).name, "t");
}

TEST(MemoTest, AddSameGroupSecondReturnsIt) {
  Memo memo;
  auto first = memo.AddGroup(logical::Table{"t"});
  auto second = memo.AddGroup(logical::Table{"t"});

  EXPECT_EQ(first->group, second->group);
  EXPECT_EQ(memo.GroupCount(), 1);
}

TEST(MemoTest, PopulateWithWholeQuery) {
  std::stringstream s{"SELECT * FROM users, orders;"};
  Operator op = GetAST(s).value();
  Memo memo;

  auto root = memo.Populate(op);

  ASSERT_EQ(memo.GroupCount(), 3);
  const auto& cross_join = std::get<logical::CrossJoin>(root->root_operator);
  EXPECT_EQ(std::get<logical::Table>(cross_join.lhs->GetLogicalExprs()[0]->root_operator).name, "users");
  EXPECT_EQ(std::get<logical::Table>(cross_join.rhs->GetLogicalExprs()[0]->root_operator).name, "orders");
}

}  // namespace stewkk::sql
