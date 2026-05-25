#include <benchmark/benchmark.h>

#include <filesystem>
#include <iostream>
#include <limits>
#include <random>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/co_spawn.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>
#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

// FIXME: ORDER BY support missing
PhysicalPlanNode ToPhysicalPlan(const Operator& op) {
  return std::visit(utils::Overloaded{
    [](const Table& t) -> PhysicalPlanNode {
      return SeqScan{.table = t.name};
    },
    [](const Projection& p) -> PhysicalPlanNode {
      return PhysicalProjection{
        .source = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*p.source)),
        .expressions = p.expressions,
      };
    },
    [](const Filter& f) -> PhysicalPlanNode {
      return PhysicalFilter{
        .source = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*f.source)),
        .predicate = f.expr,
      };
    },
    [](const CrossJoin& j) -> PhysicalPlanNode {
      return NestedLoopCrossJoin{
        .lhs = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*j.lhs)),
        .rhs = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*j.rhs)),
      };
    },
    [](const Join& j) -> PhysicalPlanNode {
      return NestedLoopJoin{
        .lhs = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*j.lhs)),
        .rhs = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*j.rhs)),
        .type = j.type,
        .qual = j.qual,
      };
    },
  }, op);
}

CardinalityEstimates MakeBenchCardinality() {
  return CardinalityEstimates({
      {"users", 17},
      {"employees", 11},
      {"employees_200", 200},
      {"departments", 5},
      {"departments_500", 1000},
      {"departments_1000", 1000},
      {"departments_2000", 2000},
      {"departments_4000", 4000},
      {"departments_8000", 8000},
      {"departments_16000", 16000},
      {"books", 3},
      {"regions", 10},
      {"customers", 500},
      {"orders", 5000},
  });
}

enum class PlannerMode { kNaive, kOptimized };

template <PlannerMode Mode>
PhysicalPlanNode MakePlan(const Operator& op) {
  if constexpr (Mode == PlannerMode::kNaive) {
    return ToPhysicalPlan(op);
  } else {
    Optimizer optimizer(op, MakeMainRules(), MakeBenchCardinality());
    return optimizer.Optimize();
  }
}

} // namespace

const static std::string kProjectDir = std::getenv("PWD");

static constexpr char kSimpleSelectSmall[]{"SELECT users.id FROM users;"};
static constexpr char kJoinSmall[]{"SELECT * FROM employees RIGHT JOIN departments ON employees.department_id = departments.id;"};
static constexpr char kComplex5[]{"SELECT departments.id*2, employees.id+1 FROM employees RIGHT JOIN departments ON employees.department_id = departments.id AND departments.id > 3 AND departments.id*2*2/2/2*2 < 30;"};
static constexpr char kComplex500[]{"SELECT departments_500.id*2, employees_200.id+1 FROM employees_200 RIGHT JOIN departments_500 ON employees_200.department_id = departments_500.id AND departments_500.id > 3 AND departments_500.id*2*2/2/2*2 < 30;"};
static constexpr char kComplex1000[]{"SELECT departments_1000.id*2, employees_200.id+1 FROM employees_200 RIGHT JOIN departments_1000 ON employees_200.department_id = departments_1000.id AND departments_1000.id > 3 AND departments_1000.id*2*2/2/2*2 < 30;"};
static constexpr char kComplex2000[]{"SELECT departments_2000.id*2, employees_200.id+1 FROM employees_200 RIGHT JOIN departments_2000 ON employees_200.department_id = departments_2000.id AND departments_2000.id > 3 AND departments_2000.id*2*2/2/2*2 < 30;"};
static constexpr char kComplex4000[]{"SELECT departments_4000.id*2, employees_200.id+1 FROM employees_200 RIGHT JOIN departments_4000 ON employees_200.department_id = departments_4000.id AND departments_4000.id > 3 AND departments_4000.id*2*2/2/2*2 < 30;"};
static constexpr char kComplex8000[]{"SELECT departments_8000.id*2, employees_200.id+1 FROM employees_200 RIGHT JOIN departments_8000 ON employees_200.department_id = departments_8000.id AND departments_8000.id > 3 AND departments_8000.id*2*2/2/2*2 < 30;"};
static constexpr char kComplex16000[]{"SELECT departments_16000.id*2, employees_200.id+1 FROM employees_200 RIGHT JOIN departments_16000 ON employees_200.department_id = departments_16000.id AND departments_16000.id > 3 AND departments_16000.id*2*2/2/2*2 < 30;"};

// 3-way joins on skewed tables (regions=10, customers=500, orders=5000).
// Naive textual order (orders ⋈ customers) ⋈ regions builds a ~2.5M-tuple
// intermediate. JoinAssociativity + JoinCommutativity let the optimizer pick a
// shape with much smaller intermediates.
static constexpr char kMultiwayOCR[]{
    "SELECT orders.id, customers.id, regions.id FROM orders "
    "JOIN customers ON orders.customer_id = customers.id "
    "JOIN regions ON customers.region_id = regions.id;"};
static constexpr char kMultiwayROC[]{
    "SELECT orders.id, customers.id, regions.id FROM regions "
    "JOIN customers ON customers.region_id = regions.id "
    "JOIN orders ON orders.customer_id = customers.id;"};

template <typename ExprExecutor, const char* Query, PlannerMode Mode>
void BM_SQL(benchmark::State& state) {
  std::ofstream nullstream("/dev/null");
  std::clog.rdbuf(nullstream.rdbuf());
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      [&state]() -> boost::asio::awaitable<void> {
        std::stringstream s{Query};
        PhysicalPlanNode op = MakePlan<Mode>(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<ExprExecutor> executor(std::move(seq_scan),
                                        co_await boost::asio::this_coro::executor);

        benchmark::DoNotOptimize(co_await executor.Execute(op));

        for (auto _ : state) {
          benchmark::DoNotOptimize(co_await executor.Execute(op));
        }
      }(),
      [](std::exception_ptr p) {});

  ctx.run();
}

#define REGISTER_BM_SQL(Query)                                                                  \
  BENCHMARK(BM_SQL<InterpretedExpressionExecutor, Query, PlannerMode::kNaive>)->UseRealTime();  \
  BENCHMARK(BM_SQL<InterpretedExpressionExecutor, Query, PlannerMode::kOptimized>)              \
      ->UseRealTime();                                                                          \
  BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, Query, PlannerMode::kNaive>)            \
      ->UseRealTime();                                                                          \
  BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, Query, PlannerMode::kOptimized>)        \
      ->UseRealTime();

REGISTER_BM_SQL(kSimpleSelectSmall)
REGISTER_BM_SQL(kJoinSmall)
REGISTER_BM_SQL(kComplex5)
REGISTER_BM_SQL(kComplex500)
REGISTER_BM_SQL(kComplex1000)
REGISTER_BM_SQL(kComplex2000)
REGISTER_BM_SQL(kComplex4000)
REGISTER_BM_SQL(kComplex8000)
REGISTER_BM_SQL(kComplex16000)
// FIXME: Optimizer search does not terminate within minutes on 3-way joins.
// Re-enable once that perf bug is fixed; see kMultiwayOCR/kMultiwayROC.
// REGISTER_BM_SQL(kMultiwayOCR)
// REGISTER_BM_SQL(kMultiwayROC)

template <typename ExprExecutor, const char* Query, PlannerMode Mode>
void BM_SQL_Multithreaded(benchmark::State& state) {
  std::ofstream nullstream("/dev/null");
  std::clog.rdbuf(nullstream.rdbuf());
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      [&state]() -> boost::asio::awaitable<void> {
        boost::asio::thread_pool pool{4};
        std::stringstream s{Query};
        PhysicalPlanNode op = MakePlan<Mode>(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<ExprExecutor> executor(std::move(seq_scan),
                                        pool.executor());

        // NOTE: precompile query
        benchmark::DoNotOptimize(co_await executor.Execute(op));

        for (auto _ : state) {
          co_await boost::asio::co_spawn(pool, [&]() -> boost::asio::awaitable<void> {
            benchmark::DoNotOptimize(co_await executor.Execute(op));
          }, boost::asio::use_awaitable);
        }
      }(),
      [](std::exception_ptr p) {});

  ctx.run();
}

#define REGISTER_BM_SQL_MT(Query)                                                                  \
  BENCHMARK(BM_SQL_Multithreaded<InterpretedExpressionExecutor, Query, PlannerMode::kNaive>)       \
      ->UseRealTime();                                                                             \
  BENCHMARK(BM_SQL_Multithreaded<InterpretedExpressionExecutor, Query, PlannerMode::kOptimized>)   \
      ->UseRealTime();                                                                             \
  BENCHMARK(BM_SQL_Multithreaded<CachedJitCompiledExpressionExecutor, Query, PlannerMode::kNaive>) \
      ->UseRealTime();                                                                             \
  BENCHMARK(                                                                                       \
      BM_SQL_Multithreaded<CachedJitCompiledExpressionExecutor, Query, PlannerMode::kOptimized>)   \
      ->UseRealTime();

REGISTER_BM_SQL_MT(kComplex5)
REGISTER_BM_SQL_MT(kComplex500)
REGISTER_BM_SQL_MT(kComplex1000)
REGISTER_BM_SQL_MT(kComplex2000)
REGISTER_BM_SQL_MT(kComplex4000)
REGISTER_BM_SQL_MT(kComplex8000)
REGISTER_BM_SQL_MT(kComplex16000)

}  // namespace stewkk::sql

BENCHMARK_MAIN();
