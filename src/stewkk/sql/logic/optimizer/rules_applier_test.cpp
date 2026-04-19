#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/rules_applier.hpp>

namespace stewkk::sql {

TEST(RulesApplierTest, ChecksThatRuleAlreadyApplied) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto* expr = join_group->GetLogicalExprs()[0].get();

  RulesApplier applier(MakeMainRules());
  constexpr TransformationRuleId kJoinCommutativity{0};

  EXPECT_TRUE(applier.IsApplicable(kJoinCommutativity, expr));
  applier.Apply(kJoinCommutativity, expr, memo);
  EXPECT_FALSE(applier.IsApplicable(kJoinCommutativity, expr));
}

TEST(RulesApplierTest, ApplyImplementationRule) {
    // FIXME
}

}  // namespace stewkk::sql
