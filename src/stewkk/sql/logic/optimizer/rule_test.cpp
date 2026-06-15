#include <gmock/gmock.h>

#include <memory>
#include <utility>

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>
#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_to_join_predicate.hpp>
#include <stewkk/sql/logic/transformation_rules/cross_join_to_join.hpp>
#include <stewkk/sql/logic/transformation_rules/projection_pushdown_through_join.hpp>
#include <stewkk/sql/logic/transformation_rules/outer_join_to_inner.hpp>
#include <stewkk/sql/logic/transformation_rules/aggregation_pushdown_through_join.hpp>
#include <stewkk/sql/logic/transformation_rules/aggregation_join_transpose.hpp>

namespace stewkk::sql {

namespace {

Expression Eq(Attribute lhs, Attribute rhs) {
  return BinaryExpression{
      std::make_shared<Expression>(std::move(lhs)),
      BinaryOp::kEq,
      std::make_shared<Expression>(std::move(rhs))};
}

Expression EqConst(Attribute lhs, IntConst rhs) {
  return BinaryExpression{
      std::make_shared<Expression>(std::move(lhs)),
      BinaryOp::kEq,
      std::make_shared<Expression>(rhs)};
}

Expression Sum(Attribute attr) {
  return AggregateExpression{
      AggregateFunction::kSum,
      std::make_shared<Expression>(std::move(attr)),
      false};
}

}  // namespace

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
    SchemaCatalog schema;
    ConstraintCatalog constraints;
};

TEST_F(JoinCommutativityTest, ReturnsSwappedJoin) {
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto expr = join_group->GetLogicalExprs()[0];

  RuleContext ctx{schema, constraints};
  auto result = rule.Apply(expr, memo, ctx);

  const auto& join = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(join.lhs.get(), b);
  EXPECT_EQ(join.rhs.get(), a);
}

TEST_F(JoinCommutativityTest, AddsNewJoinIntoGroup) {
  auto join_group = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto expr = join_group->GetLogicalExprs()[0];

  RuleContext ctx{schema, constraints};
  rule.Apply(expr, memo, ctx);

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
    SchemaCatalog schema;
    ConstraintCatalog constraints;
};

TEST_F(JoinAssociativityTest, CreatesNewGroup) {
  RuleContext ctx{schema, constraints};
  rule.Apply(abc->GetLogicalExprs()[0], memo, ctx);

  EXPECT_EQ(memo.GroupCount(), 6u);
}

TEST_F(JoinAssociativityTest, ReturnsCorrectExpression) {
  RuleContext ctx{schema, constraints};
  auto result = rule.Apply(abc->GetLogicalExprs()[0], memo, ctx);

  const auto& outer = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(outer.lhs.get(), a);
  EXPECT_EQ(outer.type, JoinType::kInner);
  const auto& inner = std::get<logical::Join>(outer.rhs->GetLogicalExprs()[0]->root_operator);
  EXPECT_EQ(inner.lhs.get(), b);
  EXPECT_EQ(inner.rhs.get(), c);
  EXPECT_EQ(inner.qual, Expression{Literal::kFalse});
  EXPECT_EQ(outer.qual, Expression{Literal::kTrue});
}

TEST_F(JoinAssociativityTest, DoesNotApplyWhenInnerJoinIsOuter) {
  auto outer_ab = memo.AddGroup(logical::Join{a, b, JoinType::kFull, Literal::kTrue})->group;
  auto expr = memo.AddGroup(
      logical::Join{outer_ab, c, JoinType::kInner, Literal::kTrue});

  RuleContext ctx{schema, constraints};
  EXPECT_FALSE(rule.IsApplicable(expr, ctx));
}

TEST_F(JoinAssociativityTest, DoesNotApplyWhenOuterJoinIsOuter) {
  auto expr = memo.AddGroup(
      logical::Join{ab, c, JoinType::kFull, Literal::kTrue});

  RuleContext ctx{schema, constraints};
  EXPECT_FALSE(rule.IsApplicable(expr, ctx));
}

TEST(TransformationRulesTest, MovesFilterIntoInnerJoinPredicate) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join = memo.AddGroup(logical::Join{a, b, JoinType::kInner, Literal::kTrue})->group;
  auto filter = memo.AddGroup(logical::Filter{join, Eq(Attribute{"a", "id"}, Attribute{"b", "id"})});
  SchemaCatalog schema;
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};
  FilterToJoinPredicate rule;

  auto result = rule.Apply(filter, memo, ctx);

  const auto& got = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(got.type, JoinType::kInner);
  EXPECT_EQ(got.qual, Eq(Attribute{"a", "id"}, Attribute{"b", "id"}));
}

TEST(TransformationRulesTest, ConvertsFilteredCrossJoinToInnerJoin) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto cross = memo.AddGroup(logical::CrossJoin{a, b})->group;
  auto pred = Eq(Attribute{"a", "id"}, Attribute{"b", "id"});
  auto filter = memo.AddGroup(logical::Filter{cross, pred});
  SchemaCatalog schema;
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};
  CrossJoinToJoin rule;

  auto result = rule.Apply(filter, memo, ctx);

  const auto& got = std::get<logical::Join>(result->root_operator);
  EXPECT_EQ(got.type, JoinType::kInner);
  EXPECT_EQ(got.qual, pred);
}

TEST(TransformationRulesTest, ConvertsNullRejectedLeftJoinToInnerJoin) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join = memo.AddGroup(logical::Join{
      a, b, JoinType::kLeft, Eq(Attribute{"a", "id"}, Attribute{"b", "aid"})})->group;
  auto filter = memo.AddGroup(logical::Filter{join, EqConst(Attribute{"b", "aid"}, 7)});
  SchemaCatalog schema;
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};
  OuterJoinToInner rule;

  auto result = rule.Apply(filter, memo, ctx);

  const auto& got_filter = std::get<logical::Filter>(result->root_operator);
  const auto& got_join = std::get<logical::Join>(
      got_filter.source->GetLogicalExprs()[0]->root_operator);
  EXPECT_EQ(got_join.type, JoinType::kInner);
}

TEST(TransformationRulesTest, PushesProjectionBelowJoinInputs) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join = memo.AddGroup(logical::Join{
      a, b, JoinType::kInner, Eq(Attribute{"a", "id"}, Attribute{"b", "aid"})})->group;
  auto projection = memo.AddGroup(logical::Projection{
      join, {Attribute{"a", "x"}}, {}});
  SchemaCatalog schema({
      {"a", {Attribute{"a", "id"}, Attribute{"a", "x"}, Attribute{"a", "unused"}}},
      {"b", {Attribute{"b", "aid"}, Attribute{"b", "unused"}}},
  });
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};
  ProjectionPushdownThroughJoin rule;

  auto result = rule.Apply(projection, memo, ctx);

  const auto& got_projection = std::get<logical::Projection>(result->root_operator);
  const auto& got_join = std::get<logical::Join>(
      got_projection.source->GetLogicalExprs()[0]->root_operator);
  EXPECT_TRUE(std::holds_alternative<logical::Projection>(
      got_join.lhs->GetLogicalExprs()[0]->root_operator));
  EXPECT_TRUE(std::holds_alternative<logical::Projection>(
      got_join.rhs->GetLogicalExprs()[0]->root_operator));
}

TEST(TransformationRulesTest, PushesPartialAggregationBelowJoin) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join = memo.AddGroup(logical::Join{
      a, b, JoinType::kInner, Eq(Attribute{"a", "id"}, Attribute{"b", "aid"})})->group;
  auto agg = memo.AddGroup(logical::Aggregation{
      join, {Attribute{"a", "id"}}, {Sum(Attribute{"a", "x"})}});
  SchemaCatalog schema;
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};
  AggregationPushdownThroughJoin rule;

  auto result = rule.Apply(agg, memo, ctx);

  const auto& final = std::get<logical::FinalAggregation>(result->root_operator);
  const auto& new_join = std::get<logical::Join>(
      final.source->GetLogicalExprs()[0]->root_operator);
  EXPECT_TRUE(std::holds_alternative<logical::PartialAggregation>(
      new_join.lhs->GetLogicalExprs()[0]->root_operator));
}

TEST(TransformationRulesTest, PartialAggregationBelowJoinKeepsOnlyLeftGroupBy) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join = memo.AddGroup(logical::Join{
      a, b, JoinType::kInner, Eq(Attribute{"a", "id"}, Attribute{"b", "aid"})})->group;
  auto agg = memo.AddGroup(logical::Aggregation{
      join,
      {Attribute{"a", "region"}, Attribute{"b", "brand"}},
      {Sum(Attribute{"a", "x"})}});
  SchemaCatalog schema;
  ConstraintCatalog constraints;
  RuleContext ctx{schema, constraints};
  AggregationPushdownThroughJoin rule;

  auto result = rule.Apply(agg, memo, ctx);

  const auto& final = std::get<logical::FinalAggregation>(result->root_operator);
  const auto& new_join = std::get<logical::Join>(
      final.source->GetLogicalExprs()[0]->root_operator);
  const auto& partial = std::get<logical::PartialAggregation>(
      new_join.lhs->GetLogicalExprs()[0]->root_operator);
  EXPECT_THAT(partial.group_by, ::testing::ElementsAre(
      Expression{Attribute{"a", "region"}},
      Expression{Attribute{"a", "id"}}));
  EXPECT_THAT(final.group_by, ::testing::ElementsAre(
      Expression{Attribute{"a", "region"}},
      Expression{Attribute{"b", "brand"}}));
}

TEST(TransformationRulesTest, TransposesAggregationAndJoinWithConstraints) {
  Memo memo;
  auto a = memo.AddGroup(logical::Table{"a"})->group;
  auto b = memo.AddGroup(logical::Table{"b"})->group;
  auto join = memo.AddGroup(logical::Join{
      a, b, JoinType::kInner, Eq(Attribute{"a", "id"}, Attribute{"b", "id"})})->group;
  auto agg = memo.AddGroup(logical::Aggregation{
      join, {Attribute{"a", "id"}}, {Sum(Attribute{"a", "x"})}});
  SchemaCatalog schema;
  ConstraintCatalog constraints(
      {UniqueKeyInfo{.table = "b", .column = "id"}},
      {ForeignKeyInfo{.from_table = "a", .from_column = "id",
                      .to_table = "b", .to_column = "id"}});
  RuleContext ctx{schema, constraints};
  AggregationJoinTranspose rule;

  auto result = rule.Apply(agg, memo, ctx);

  const auto& projection = std::get<logical::Projection>(result->root_operator);
  const auto& new_join = std::get<logical::Join>(
      projection.source->GetLogicalExprs()[0]->root_operator);
  EXPECT_TRUE(std::holds_alternative<logical::Aggregation>(
      new_join.lhs->GetLogicalExprs()[0]->root_operator));
}

}  // namespace stewkk::sql
