#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>
#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>

namespace stewkk::sql {

class JoinCommutativityTest : public ::testing::Test {
  protected:
    void SetUp() override {
      a = memo.AddGroup(logical::Table{"a"})->group;
      b = memo.AddGroup(logical::Table{"b"})->group;
    }

    Memo memo;
    Group* a = nullptr;
    Group* b = nullptr;
    JoinCommutativity rule;
};

TEST_F(JoinCommutativityTest, ReturnsSwappedJoin) {
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto expr = join_group->GetLogicalExprs()[0];

  auto result = rule.Apply(expr, memo);

  const auto& join = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(join.lhs.get(), b);
  EXPECT_EQ(join.rhs.get(), a);
}

TEST_F(JoinCommutativityTest, AddsNewJoinIntoGroup) {
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto expr = join_group->GetLogicalExprs()[0];

  rule.Apply(expr, memo);

  EXPECT_EQ(join_group->GetLogicalExprs().size(), 2u);
  const auto& new_join = std::get<logical::Join>(join_group->GetLogicalExprs()[1]->root_operator);
  EXPECT_EQ(new_join.lhs.get(), b);
  EXPECT_EQ(new_join.rhs.get(), a);
}

class JoinAssociativityTest : public ::testing::Test {
  protected:
    void SetUp() override {
      a   = memo.AddGroup(logical::Table{"a"})->group;
      b   = memo.AddGroup(logical::Table{"b"})->group;
      c   = memo.AddGroup(logical::Table{"c"})->group;
      ab  = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
      abc = memo.AddGroup(logical::Join{ab, c, JoinType::kInner, Literal::kFalse})->group;
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
  rule.Apply(abc->GetLogicalExprs()[0], memo);

  EXPECT_EQ(memo.GroupCount(), 6u);
}

TEST_F(JoinAssociativityTest, ReturnsCorrectExpression) {
  auto result = rule.Apply(abc->GetLogicalExprs()[0], memo);

  const auto& outer = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(outer.lhs.get(), a);
  EXPECT_EQ(outer.type, JoinType::kInner);
  const auto& inner = std::get<logical::Join>(outer.rhs->GetLogicalExprs()[0]->root_operator);
  EXPECT_EQ(inner.lhs.get(), b);
  EXPECT_EQ(inner.rhs.get(), c);
  EXPECT_EQ(inner.qual, Expression{Literal::kTrue});
}

}  // namespace stewkk::sql
