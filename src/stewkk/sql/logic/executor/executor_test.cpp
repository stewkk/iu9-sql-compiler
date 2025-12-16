#include <gmock/gmock.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>

using ::testing::Eq;

namespace stewkk::sql {

const static std::string kProjectDir = std::getenv("PWD");

TEST(ExecutorTest, SimpleSelect) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<std::vector<Tuple>>> {
        std::stringstream s{"SELECT * FROM users;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        auto io_executor = co_await boost::asio::this_coro::executor;
        Executor executor(io_executor, std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<std::vector<Tuple>> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value()[0], Eq(Tuple{{"users", "id", 1}, {"users", "age", 33}}));
        ASSERT_THAT(got.value().size(), Eq(17));
      });

  ctx.run();
}

}  // namespace stewkk::sql
