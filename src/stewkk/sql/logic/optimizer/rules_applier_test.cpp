#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/rules_applier.hpp>

namespace stewkk::sql {

TEST(RulesApplierTest, ChecksThatRuleAlreadyApplied) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto expr = join_group->GetLogicalExprs()[0];

  RulesApplier applier(MakeMainRules());
  constexpr TransformationRuleId kJoinCommutativity{0};
  SchemaCatalog schema;
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};

  EXPECT_TRUE(applier.IsApplicable(kJoinCommutativity, expr, ctx));
  applier.Apply(kJoinCommutativity, expr, memo, ctx);
  EXPECT_FALSE(applier.IsApplicable(kJoinCommutativity, expr, ctx));
}

TEST(RulesApplierTest, ApplyImplementationRule) {
    // FIXME
}

}  // namespace stewkk::sql
