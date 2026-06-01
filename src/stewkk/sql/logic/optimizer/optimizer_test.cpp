#include <gmock/gmock.h>

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

TEST(OptimizerTest, Simple) {
  std::stringstream s{"SELECT * FROM users;"};
  Operator op = GetAST(s).value().op;
  Optimizer optimizer(op, MakeMainRules());

  auto got = optimizer.Optimize();

  ASSERT_THAT(SerializeDot(got), Eq("digraph G { rankdir=BT;\n  n0 [label=\"SeqScan\\\\nusers\"]\n}\n"));
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
         " (SeqScan orders) (HashJoin Inner (= (attr customers region_id) (attr regions id))"
         " (SeqScan regions) (SeqScan customers))))"));
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
         " (SeqScan customers) (SeqScan regions)) (SeqScan orders)))"));
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
