#include <gmock/gmock.h>

#include <string_view>
#include <unordered_map>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/reachability.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>
#include <stewkk/sql/logic/executor/plan_serializer.hpp>

using ::testing::Eq;
using ::testing::IsTrue;
using ::testing::IsFalse;
using ::testing::HasSubstr;

namespace stewkk::sql {

// FIXME: branch and bound
// FIXME: сделать API в виде DoStep(), которое возвращает какой-то внутренний стейт оптимизатора
// FIXME: применение правила (по крайней мере трансформации), должно создавать несколько выражений

int64_t EstimateCardinality(
    std::string_view sql, std::unordered_map<std::string, int64_t> table_sizes) {
  std::stringstream s{std::string{sql}};
  Memo memo;
  auto root = memo.Populate(GetAST(s).value().op);
  CardinalityEstimates cardinality{std::move(table_sizes)};
  return cardinality.GetCardinality(root->group);
}

TEST(CardinalityEstimatesTest, AppliesFilterHeuristics) {
  ASSERT_THAT(EstimateCardinality("SELECT * FROM users WHERE users.id = 1;", {{"users", 1000}}),
              Eq(100));
  ASSERT_THAT(
      EstimateCardinality("SELECT * FROM users WHERE users.age BETWEEN 18 AND 30;",
                          {{"users", 1000}}),
      Eq(250));
  ASSERT_THAT(
      EstimateCardinality("SELECT * FROM users WHERE users.id IN (1, 2, 3);",
                          {{"users", 1000}}),
      Eq(300));
}

TEST(SchemaCatalogTest, DerivesJoinWidth) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Memo memo;
  auto root = memo.Populate(GetAST(s).value().op);
  SchemaCatalog schema({
      {"users", {Attribute{"users", "id"}, Attribute{"users", "name"}}},
      {"orders", {Attribute{"orders", "id"}, Attribute{"orders", "user_id"},
                  Attribute{"orders", "total"}}},
  });

  ASSERT_THAT(schema.GetWidth(root->group), Eq(5));
}

TEST(OptimizerTest, Simple) {
  std::stringstream s{"SELECT * FROM users;"};
  Operator op = GetAST(s).value().op;
  Optimizer optimizer(op, MakeMainRules());

  auto got = optimizer.Optimize();

  ASSERT_THAT(SerializeDot(got), Eq("digraph G { rankdir=BT;\n  n0 [label=\"SeqScan\\\\nusers\"]\n}\n"));
  ASSERT_THAT(optimizer.GetBestCost(), Eq(1000));
}

TEST(OptimizerTest, JoinCommutativity) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Operator op = GetAST(s).value().op;
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"users", 10000},
      {"orders", 100},
  }));


  auto got = optimizer.Optimize();

  ASSERT_THAT(Serialize(got), Eq("(HashJoin Inner (= (attr users id) (attr orders user_id)) (SeqScan orders) (SeqScan users))"));
}

TEST(OptimizerTest, MultiwayJoinOCR) {
  std::stringstream s{
      "SELECT orders.id, customers.id, regions.id FROM orders "
      "JOIN customers ON orders.customer_id = customers.id "
      "JOIN regions ON customers.region_id = regions.id;"};
  Operator op = GetAST(s).value().op;
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"regions", 10},
      {"customers", 500},
      {"orders", 5000},
  }));

  auto got = optimizer.Optimize();

  ASSERT_THAT(
      Serialize(got),
      Eq("(PhysicalProjection (exprs (attr orders id) (attr customers id) (attr regions id))"
         " (HashJoin Inner (= (attr orders customer_id) (attr customers id))"
         " (HashJoin Inner (= (attr customers region_id) (attr regions id))"
         " (SeqScan regions) (SeqScan customers)) (SeqScan orders)))"));
}

TEST(OptimizerTest, MultiwayJoinROC) {
  std::stringstream s{
      "SELECT orders.id, customers.id, regions.id FROM regions "
      "JOIN customers ON customers.region_id = regions.id "
      "JOIN orders ON orders.customer_id = customers.id;"};
  Operator op = GetAST(s).value().op;
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"regions", 10},
      {"customers", 500},
      {"orders", 5000},
  }));

  auto got = optimizer.Optimize();

  ASSERT_THAT(
      Serialize(got),
      Eq("(PhysicalProjection (exprs (attr orders id) (attr customers id) (attr regions id))"
         " (HashJoin Inner (= (attr orders customer_id) (attr customers id))"
         " (HashJoin Inner (= (attr customers region_id) (attr regions id))"
         " (SeqScan regions) (SeqScan customers)) (SeqScan orders)))"));
}

TEST(OptimizerTest, OrderBy) {
  std::stringstream s{"SELECT * FROM users ORDER BY users.id;"};
  auto parsed = GetAST(s).value();
  SchemaCatalog schema({{"users", {Attribute{"users", "id"}, Attribute{"users", "age"}}}});
  PropertySet required = parsed.required_order
      ? PropertySet{SortProperty{*parsed.required_order}}
      : PropertySet::Any();
  Optimizer optimizer(parsed.op, MakeMainRules(), {}, std::move(schema), std::move(required));

  auto got = optimizer.Optimize();

  ASSERT_THAT(Serialize(got), Eq("(Sort (keys users.id Asc) (SeqScan users))"));
}

TEST(OptimizerTest, OrderBySortsAfterCrossJoin) {
  std::stringstream s{
      "SELECT * FROM departments CROSS JOIN orders ORDER BY departments.id;"};
  auto parsed = GetAST(s).value();
  SchemaCatalog schema({
      {"departments", {Attribute{"departments", "id"}}},
      {"orders", {Attribute{"orders", "id"}}},
  });
  PropertySet required{SortProperty{*parsed.required_order}};
  Optimizer optimizer(parsed.op, MakeMainRules(), {}, std::move(schema), std::move(required));

  auto got = optimizer.Optimize();

  ASSERT_THAT(
      Serialize(got),
      Eq("(Sort (keys departments.id Asc)"
         " (NestedLoopCrossJoin (SeqScan departments) (SeqScan orders)))"));
}

TEST(OptimizerTest, OrderBySortsAfterNestedLoopJoin) {
  std::stringstream s{
      "SELECT * FROM departments JOIN orders ON departments.id < orders.id "
      "ORDER BY departments.id;"};
  auto parsed = GetAST(s).value();
  SchemaCatalog schema({
      {"departments", {Attribute{"departments", "id"}}},
      {"orders", {Attribute{"orders", "id"}}},
  });
  PropertySet required{SortProperty{*parsed.required_order}};
  Optimizer optimizer(parsed.op, MakeMainRules(), {}, std::move(schema), std::move(required));

  auto got = optimizer.Optimize();

  ASSERT_THAT(
      Serialize(got),
      HasSubstr("(Sort (keys departments.id Asc) (NestedLoopJoin Inner"));
}

TEST(OptimizerTest, AliasedJoinOptimizesWithAliasQualifiedAttrs) {
  std::stringstream s{"SELECT c.id FROM customers AS c JOIN orders AS o ON c.id = o.customer_id WHERE c.region_id = 1;"};
  Operator op = GetAST(s).value().op;
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"customers", 500},
      {"orders", 5000},
  }));

  auto got = optimizer.Optimize();
  auto serialized = Serialize(got);

  ASSERT_THAT(serialized, HasSubstr("(SeqScan customers c)"));
  ASSERT_THAT(serialized, HasSubstr("(SeqScan orders o)"));
  ASSERT_THAT(serialized, HasSubstr("(attr c id)"));
  ASSERT_THAT(serialized, HasSubstr("(attr o customer_id)"));
}

TEST(OptimizerTest, PushesSelectiveFilterIntoHashBuildSide) {
  std::stringstream s{
      "SELECT * FROM lineorder AS lo "
      "JOIN supplier AS s ON lo.suppkey = s.id "
      "WHERE s.region = 'AMERICA';"};
  Operator op = GetAST(s).value().op;
  SchemaCatalog schema({
      {"lineorder", {Attribute{"lineorder", "suppkey"}, Attribute{"lineorder", "value"}}},
      {"supplier", {Attribute{"supplier", "id"}, Attribute{"supplier", "region"}}},
  });
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"lineorder", 6000},
      {"supplier", 100},
  }), std::move(schema));

  auto got = optimizer.Optimize();

  ASSERT_THAT(
      Serialize(got),
      Eq("(HashJoin Inner (= (attr lo suppkey) (attr s id))"
         " (PhysicalFilter (= (attr s region) (str \"AMERICA\")) (SeqScan supplier s))"
         " (SeqScan lineorder lo))"));
}

TEST(ReachabilityTest, SeqScanReachable) {
  std::stringstream s{"SELECT * FROM users;"};
  auto result = IsPlanReachable(s, SeqScan{"users"});
  ASSERT_THAT(result.reachable, IsTrue());
}

TEST(ReachabilityTest, SeqScanWrongTable) {
  std::stringstream s{"SELECT * FROM users;"};
  auto result = IsPlanReachable(s, SeqScan{"orders"});
  ASSERT_THAT(result.reachable, IsFalse());
  ASSERT_THAT(result.mismatch, HasSubstr("users"));
}

TEST(ReachabilityTest, WrongOperatorType) {
  std::stringstream s{"SELECT * FROM users;"};
  Expression qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"orders", "user_id"})};
  auto result = IsPlanReachable(s, HashJoin{
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
      std::make_shared<PhysicalPlanNode>(SeqScan{"orders"}),
      JoinType::kInner,
      qual});
  ASSERT_THAT(result.reachable, IsFalse());
}

TEST(ReachabilityTest, BothJoinOrdersReachable) {
  std::stringstream s1{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  std::stringstream s2{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Expression qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"orders", "user_id"})};
  CardinalityEstimates cardinality({{"users", 10000}, {"orders", 100}});

  auto optimal = IsPlanReachable(s1, NestedLoopJoin{
      std::make_shared<PhysicalPlanNode>(SeqScan{"orders"}),
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
      JoinType::kInner, qual}, cardinality);
  ASSERT_THAT(optimal.reachable, IsTrue());

  auto suboptimal = IsPlanReachable(s2, NestedLoopJoin{
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
      std::make_shared<PhysicalPlanNode>(SeqScan{"orders"}),
      JoinType::kInner, qual}, cardinality);
  ASSERT_THAT(suboptimal.reachable, IsTrue());
}

TEST(ReachabilityTest, HashJoinReachable) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Expression qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"orders", "user_id"})};
  auto result = IsPlanReachable(s, HashJoin{
      std::make_shared<PhysicalPlanNode>(SeqScan{"orders"}),
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
      JoinType::kInner, qual});
  ASSERT_THAT(result.reachable, IsTrue());
}

TEST(ReachabilityTest, WrongJoinQual) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Expression wrong_qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"orders", "id"})};
  auto result = IsPlanReachable(s, NestedLoopJoin{
      std::make_shared<PhysicalPlanNode>(SeqScan{"orders"}),
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
      JoinType::kInner, wrong_qual});
  ASSERT_THAT(result.reachable, IsFalse());
  ASSERT_THAT(result.mismatch, HasSubstr("qual"));
}

TEST(ReachabilityTest, InExpandsToOrChain) {
  std::stringstream s{"SELECT * FROM users WHERE users.id IN (1, 2, 3);"};
  Expression eq1 = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(IntConst{1})};
  Expression eq2 = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(IntConst{2})};
  Expression eq3 = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(IntConst{3})};
  Expression or_chain = BinaryExpression{
      std::make_shared<Expression>(BinaryExpression{
          std::make_shared<Expression>(std::move(eq1)),
          BinaryOp::kOr,
          std::make_shared<Expression>(std::move(eq2))}),
      BinaryOp::kOr,
      std::make_shared<Expression>(std::move(eq3))};
  auto result = IsPlanReachable(s, PhysicalFilter{
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}), or_chain});
  ASSERT_THAT(result.reachable, IsTrue());
}



TEST(ReachabilityTest, OrderByReachableViaSortEnforcer) {
  std::stringstream s{"SELECT users.id FROM users ORDER BY users.id;"};
  PhysicalSort target{
      std::make_shared<PhysicalPlanNode>(PhysicalProjection{
          std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
          {Attribute{"users", "id"}},
          {}}),
      SortOrder{{SortKey{"users", "id", Direction::kAsc}}}};
  auto result = IsPlanReachable(s, target);
  ASSERT_THAT(result.reachable, IsTrue());
}



TEST(ReachabilityTest, OrderByWrongDirectionNotReachable) {
  std::stringstream s{"SELECT users.id FROM users ORDER BY users.id ASC;"};
  PhysicalSort target{
      std::make_shared<PhysicalPlanNode>(PhysicalProjection{
          std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
          {Attribute{"users", "id"}},
          {}}),
      SortOrder{{SortKey{"users", "id", Direction::kDesc}}}};
  auto result = IsPlanReachable(s, target);
  ASSERT_THAT(result.reachable, IsFalse());
}



TEST(ReachabilityTest, SortNotReachableWithoutOrderBy) {
  std::stringstream s{"SELECT users.id FROM users;"};
  PhysicalSort target{
      std::make_shared<PhysicalPlanNode>(PhysicalProjection{
          std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
          {Attribute{"users", "id"}},
          {}}),
      SortOrder{{SortKey{"users", "id", Direction::kAsc}}}};
  auto result = IsPlanReachable(s, target);
  ASSERT_THAT(result.reachable, IsFalse());
}

}  // namespace stewkk::sql
