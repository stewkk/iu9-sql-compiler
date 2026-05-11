#include <gmock/gmock.h>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/executor/plan_serializer.hpp>

using ::testing::Eq;

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

  ASSERT_THAT(Serialize(got), ::testing::HasSubstr("(SeqScan orders) (SeqScan users)"));
}

}  // namespace stewkk::sql
