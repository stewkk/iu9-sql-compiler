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

namespace {
}  // namespace

template <typename T>
void BM_SimpleSelect(benchmark::State& state) {
  std::ofstream nullstream("/dev/null");
  std::clog.rdbuf(nullstream.rdbuf());
  for (auto _ : state) {
    boost::asio::io_context ctx;
    boost::asio::co_spawn(
        ctx,
        []() -> boost::asio::awaitable<void> {
          std::stringstream s{"SELECT * FROM users;"};
          Operator op = GetAST(s).value();
          CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
          Executor<T> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

          benchmark::DoNotOptimize(co_await executor.Execute(op));
        }(),
        [](std::exception_ptr p) {
        });

    ctx.run();
  }
}

BENCHMARK(BM_SimpleSelect<InterpretedExpressionExecutor>);
BENCHMARK(BM_SimpleSelect<JitCompiledExpressionExecutor>);

}  // namespace stewkk::sql

BENCHMARK_MAIN();
