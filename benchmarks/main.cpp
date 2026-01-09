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
static constexpr char kComplexSmall[]{"SELECT departments.id*2, employees.id+1 FROM employees RIGHT JOIN departments ON employees.department_id = departments.id AND departments.id > 3 AND departments.id*2*2/2 < 30;"};

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

        for (auto _ : state) {
          benchmark::DoNotOptimize(co_await executor.Execute(op));
        }
      }(),
      [](std::exception_ptr p) {});

  ctx.run();
}

BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kSimpleSelectSmall>);
BENCHMARK(BM_SQL<JitCompiledExpressionExecutor, kSimpleSelectSmall>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kSimpleSelectSmall>);
BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kJoinSmall>);
BENCHMARK(BM_SQL<JitCompiledExpressionExecutor, kJoinSmall>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kJoinSmall>);
BENCHMARK(BM_SQL<InterpretedExpressionExecutor, kComplexSmall>);
BENCHMARK(BM_SQL<JitCompiledExpressionExecutor, kComplexSmall>);
BENCHMARK(BM_SQL<CachedJitCompiledExpressionExecutor, kComplexSmall>);

}  // namespace stewkk::sql

BENCHMARK_MAIN();
