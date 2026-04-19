#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>
#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>

namespace stewkk::sql {

class JoinCommutativityTest : public ::testing::Test {
  protected:
    void SetUp() override {
      a = memo.AddGroup(logical::Table{"a"});
      b = memo.AddGroup(logical::Table{"b"});
    }

    Memo memo;
    Group* a = nullptr;
    Group* b = nullptr;
    JoinCommutativity rule;
};

TEST_F(JoinCommutativityTest, ReturnsSwappedJoin) {
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue});
  auto* expr = join_group->GetLogicalExprs()[0].get();

  auto result = rule.Apply(expr, memo);

  const auto& join = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(join.lhs.get(), b);
  EXPECT_EQ(join.rhs.get(), a);
}

TEST_F(JoinCommutativityTest, AddsNewJoinIntoGroup) {
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue});
  auto* expr = join_group->GetLogicalExprs()[0].get();

  rule.Apply(expr, memo);

  EXPECT_EQ(join_group->GetLogicalExprs().size(), 2u);
  const auto& new_join = std::get<logical::Join>(join_group->GetLogicalExprs()[1].get()->root_operator);
  EXPECT_EQ(new_join.lhs.get(), b);
  EXPECT_EQ(new_join.rhs.get(), a);
}

class JoinAssociativityTest : public ::testing::Test {
  protected:
    void SetUp() override {
      a  = memo.AddGroup(logical::Table{"a"});
      b  = memo.AddGroup(logical::Table{"b"});
      c  = memo.AddGroup(logical::Table{"c"});
      ab = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue});
      abc = memo.AddGroup(logical::Join{ab, c, JoinType::kInner, Literal::kFalse});
    }

    Memo memo;
    Group* a = nullptr;
    Group* b = nullptr;
    Group* c = nullptr;
    Group* ab = nullptr;
    Group* abc = nullptr;
    JoinAssociativity rule;
};

TEST_F(JoinAssociativityTest, CreatesNewGroup) {
  rule.Apply(abc->GetLogicalExprs()[0].get(), memo);

  EXPECT_EQ(memo.GroupCount(), 6u);
}

TEST_F(JoinAssociativityTest, ReturnsCorrectExpression) {
  auto result = rule.Apply(abc->GetLogicalExprs()[0].get(), memo);

  const auto& outer = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(outer.lhs.get(), a);
  EXPECT_EQ(outer.type, JoinType::kInner);
  const auto& inner = std::get<logical::Join>(outer.rhs->GetLogicalExprs()[0].get()->root_operator);
  EXPECT_EQ(inner.lhs.get(), b);
  EXPECT_EQ(inner.rhs.get(), c);
  EXPECT_EQ(inner.qual, Expression{Literal::kTrue});
}

}  // namespace stewkk::sql
