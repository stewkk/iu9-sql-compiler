#include <benchmark/benchmark.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>
#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

PhysicalPlanNode ToPhysicalPlan(const Operator& op) {
  return std::visit(utils::Overloaded{
    [](const Table& t) -> PhysicalPlanNode {
      return SeqScan{.table = t.name, .alias = t.alias};
    },
    [](const Projection& p) -> PhysicalPlanNode {
      return PhysicalProjection{
        .source = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*p.source)),
        .expressions = p.expressions,
        .aliases = p.aliases,
      };
    },
    [](const Filter& f) -> PhysicalPlanNode {
      return PhysicalFilter{
        .source = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*f.source)),
        .predicate = f.expr,
      };
    },
    [](const Aggregation& a) -> PhysicalPlanNode {
      return PhysicalAggregation{
        .source = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*a.source)),
        .group_by = a.group_by,
        .aggregates = a.aggregates,
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

CardinalityEstimates LoadCardinalityFromCsvDir(const std::filesystem::path& dir) {
  std::unordered_map<std::string, int64_t> counts;
  if (!std::filesystem::is_directory(dir)) {
    return CardinalityEstimates{};
  }
  for (const auto& entry : std::filesystem::directory_iterator{dir}) {
    if (entry.path().extension() != ".csv") continue;
    std::ifstream in{entry.path()};
    std::string line;
    int64_t rows = -1;
    while (std::getline(in, line)) ++rows;
    counts.emplace(entry.path().stem().string(), std::max<int64_t>(0, rows));
  }
  return CardinalityEstimates{std::move(counts)};
}

class ScopedClogSuppression {
public:
  ScopedClogSuppression() : nullstream_("/dev/null"), previous_(std::clog.rdbuf(nullstream_.rdbuf())) {}
  ~ScopedClogSuppression() { std::clog.rdbuf(previous_); }

private:
  std::ofstream nullstream_;
  std::streambuf* previous_;
};

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

template <PlannerMode Mode>
PhysicalPlanNode MakePlan(const ParsedQuery& query, CardinalityEstimates cardinality) {
  if constexpr (Mode == PlannerMode::kNaive) {
    return ToPhysicalPlan(query.op);
  } else {
    PropertySet required = query.required_order
        ? PropertySet{SortProperty{*query.required_order}}
        : PropertySet::Any();
    Optimizer optimizer(query.op, MakeMainRules(), std::move(cardinality), {},
                        std::move(required));
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
  ScopedClogSuppression suppress_clog;
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
REGISTER_BM_SQL(kMultiwayOCR)
REGISTER_BM_SQL(kMultiwayROC)

template <typename ExprExecutor, const char* Query, PlannerMode Mode>
void BM_SQL_Multithreaded(benchmark::State& state) {
  ScopedClogSuppression suppress_clog;
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

namespace {

std::string ReadTextFile(const std::filesystem::path& path) {
  std::ifstream in{path};
  std::ostringstream out;
  out << in.rdbuf();
  return out.str();
}

std::filesystem::path SsbDataDir() {
  if (const char* env = std::getenv("SSB_DATA_DIR")) {
    return env;
  }
  return std::filesystem::path{kProjectDir} / "benchmarks/datasets/ssb/generated/sf1";
}

template <typename ExprExecutor, PlannerMode Mode>
void BM_SSB(benchmark::State& state, std::string query, std::filesystem::path data_dir) {
  ScopedClogSuppression suppress_clog;

  if (!std::filesystem::is_directory(data_dir)) {
    state.SkipWithError(("SSB data directory not found: " + data_dir.string()
                         + " (set SSB_DATA_DIR)").c_str());
    return;
  }

  std::stringstream sql{query};
  auto parsed = GetAST(sql);
  if (!parsed.has_value()) {
    state.SkipWithError(("SSB query parse failed: " + What(parsed.error())).c_str());
    return;
  }

  PhysicalPlanNode plan;
  try {
    plan = MakePlan<Mode>(parsed.value(), LoadCardinalityFromCsvDir(data_dir));
  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
    return;
  }

  boost::asio::io_context ctx;
  CsvDirSequentialScanner seq_scan{data_dir.string()};
  Executor<ExprExecutor> executor(std::move(seq_scan), ctx.get_executor());

  auto run_once = [&]() {
    auto fut = boost::asio::co_spawn(ctx, executor.Execute(plan), boost::asio::use_future);
    ctx.restart();
    ctx.run();
    return fut.get();
  };

  try {
    benchmark::DoNotOptimize(run_once());
    for (auto _ : state) {
      benchmark::DoNotOptimize(run_once());
    }
  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

struct SsbRegistration {
  SsbRegistration() {
    const auto query_dir = std::filesystem::path{kProjectDir} / "benchmarks/datasets/ssb/queries";
    const auto data_dir = SsbDataDir();
    if (!std::filesystem::is_directory(query_dir)) return;

    for (const auto& entry : std::filesystem::directory_iterator{query_dir}) {
      if (entry.path().extension() != ".sql") continue;
      auto query = ReadTextFile(entry.path());
      auto name = "SSB/" + entry.path().stem().string();
      benchmark::RegisterBenchmark(
          (name + "/Interpreted/Naive").c_str(),
          &BM_SSB<InterpretedExpressionExecutor, PlannerMode::kNaive>, query, data_dir)
          ->UseRealTime();
      benchmark::RegisterBenchmark(
          (name + "/Interpreted/Optimized").c_str(),
          &BM_SSB<InterpretedExpressionExecutor, PlannerMode::kOptimized>, query, data_dir)
          ->UseRealTime();
    }
  }
};

const SsbRegistration kRegisterSsb;

} // namespace

}  // namespace stewkk::sql

BENCHMARK_MAIN();
