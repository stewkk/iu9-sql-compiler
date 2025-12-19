#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/co_spawn.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>

using ::testing::Eq;

namespace stewkk::sql {

const static std::string kProjectDir = std::getenv("PWD");

namespace {

std::string ReadFromFile(std::filesystem::path path) {
  std::ifstream f{path};
  std::ostringstream stream;
  stream << f.rdbuf();
  return stream.str();
}

} // namespace

TEST(ExecutorTest, SimpleSelect) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM users;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}, {"users", "age", Type::kInt}}));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][0]).int_value, Eq(1));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][1]).int_value, Eq(33));
        ASSERT_THAT(got.value().tuples.size(), Eq(17));
      });

  ctx.run();
}

TEST(ExecutorTest, SimpleSelectWithParallelism) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM users;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}, {"users", "age", Type::kInt}}));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][0]).int_value, Eq(1));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][1]).int_value, Eq(33));
        ASSERT_THAT(got.value().tuples.size(), Eq(17));
      });

  pool.join();
}

TEST(ExecutorTest, Projection) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[1][0]).int_value, Eq(2));
        ASSERT_THAT(got.value().tuples.size(), Eq(17));
      });

  pool.join();
}

TEST(ExecutorTest, Filter) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users WHERE users.age < 10;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][0]).int_value, Eq(5));
        ASSERT_THAT(got.value().tuples.size(), Eq(2));
      });

  pool.join();
}

TEST(ExecutorTest, FilterMany) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users WHERE users.age > 10;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][0]).int_value, Eq(1));
        ASSERT_THAT(got.value().tuples.size(), Eq(15));
      });

  pool.join();
}

TEST(ExecutorTest, CrossJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM users, books WHERE users.age < 10;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{
                                                {"users", "id", Type::kInt},
                                                {"users", "age", Type::kInt},
                                                {"books", "id", Type::kInt},
                                                {"books", "price", Type::kInt},
                                            }));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][3]).int_value, Eq(55));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][0]).int_value, Eq(5));
        ASSERT_THAT(got.value().tuples.size(), Eq(6));
      });

  pool.join();
}

TEST(ExecutorTest, InnerJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM employees JOIN departments ON employees.department_id = departments.id;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{
                                                {"employees", "id", Type::kInt},
                                                {"employees", "department_id", Type::kInt},
                                                {"departments", "id", Type::kInt},
                                            }));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][0]).int_value, Eq(1));
        ASSERT_THAT(std::get<NonNullValue>(got.value().tuples[0][2]).int_value, Eq(3));
        ASSERT_THAT(got.value().tuples.size(), Eq(3));
        ASSERT_THAT(ToString(got.value()), Eq(ReadFromFile(kProjectDir+"/test/static/executor/expected_inner_join.txt")));
      });

  pool.join();
}

TEST(ExecutorTest, LeftJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM employees LEFT OUTER JOIN departments ON employees.department_id = departments.id;"};
        Operator op = GetAST(s).value();
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan));

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{
                                                {"departments", "id", Type::kInt},
                                                {"employees", "id", Type::kInt},
                                                {"employees", "department_id", Type::kInt},
                                            }));
        ASSERT_THAT(got.value().tuples.size(), Eq(11));
        ASSERT_THAT(ToString(got.value()), Eq(ReadFromFile(kProjectDir+"/test/static/executor/expected_left_join.txt")));
      });

  pool.join();
}

}  // namespace stewkk::sql
