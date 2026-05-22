#include <gmock/gmock.h>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/reachability.hpp>
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
  Operator op = GetAST(s).value();
  Optimizer optimizer(op, MakeMainRules());

  auto got = optimizer.Optimize();
  
  ASSERT_THAT(SerializeDot(got), Eq("digraph G { rankdir=BT;\n  n0 [label=\"SeqScan\\\\nusers\"]\n}\n"));
}

TEST(OptimizerTest, JoinCommutativity) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Operator op = GetAST(s).value();
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"users", 10000},
      {"orders", 100},
  }));


  auto got = optimizer.Optimize();

  ASSERT_THAT(Serialize(got), Eq("(NestedLoopJoin Inner (= (attr users id) (attr orders user_id)) (SeqScan orders) (SeqScan users))"));
}

TEST(OptimizerTest, OrderBy) {
   // TODO: SELECT a FROM T ORDER BY b
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

TEST(ReachabilityTest, HashJoinNotReachable) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Expression qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"users", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"orders", "user_id"})};
  auto result = IsPlanReachable(s, HashJoin{
      std::make_shared<PhysicalPlanNode>(SeqScan{"orders"}),
      std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
      JoinType::kInner, qual});
  ASSERT_THAT(result.reachable, IsFalse());
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

}  // namespace stewkk::sql
