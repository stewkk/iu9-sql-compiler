#include <benchmark/benchmark.h>

#include <filesystem>
#include <limits>
#include <random>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/co_spawn.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>

namespace stewkk::sql {

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

template <typename ExprExecutor, const char* Query>
void BM_SQL(benchmark::State& state) {
  std::ofstream nullstream("/dev/null");
  std::clog.rdbuf(nullstream.rdbuf());
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      [&state]() -> boost::asio::awaitable<void> {
        std::stringstream s{Query};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<ExprExecutor> executor(std::move(seq_scan),
                                        co_await boost::asio::this_coro::executor);

        // NOTE: precompile query
        benchmark::DoNotOptimize(co_await executor.Execute(op));

        for (auto _ : state) {
          benchmark::DoNotOptimize(co_await executor.Execute(op));
        }
      }(),
      [](std::exception_ptr p) {});

  ctx.run();
}

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kSimpleSelectSmall>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kSimpleSelectSmall>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kJoinSmall>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kJoinSmall>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex5>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex5>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex500>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex500>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex1000>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex1000>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex2000>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex2000>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex4000>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex4000>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex8000>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex8000>);

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplex16000>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplex16000>);

}  // namespace stewkk::sql

BENCHMARK_MAIN();
