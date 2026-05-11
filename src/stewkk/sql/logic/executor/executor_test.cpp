#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/co_spawn.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/utils/overloaded.hpp>

using ::testing::Eq;

namespace stewkk::sql {

namespace {

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

} // namespace

const static std::string kProjectDir = std::getenv("PWD");

namespace {

std::string ReadFromFile(std::filesystem::path path) {
  std::ifstream f{path};
  std::ostringstream stream;
  stream << f.rdbuf();
  return stream.str();
}

} // namespace

template <typename T>
class ExecutorTest : public testing::Test {};

TYPED_TEST_SUITE_P(ExecutorTest);

TEST(ExecutorTest, SimpleSelect) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM users;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}, {"users", "age", Type::kInt}}));
        ASSERT_THAT(got.value().tuples[0], Eq(Tuple{Value{false, 1}, Value{false, 33}}));
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}, {"users", "age", Type::kInt}}));
        ASSERT_THAT(got.value().tuples[0], Eq(Tuple{Value{false, 1}, Value{false, 33}}));
        ASSERT_THAT(got.value().tuples.size(), Eq(17));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, Projection) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(got.value().tuples[1], Eq(Tuple{Value{false, 2}}));
        ASSERT_THAT(got.value().tuples.size(), Eq(17));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, Filter) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users WHERE users.age < 10;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(got.value().tuples[0], Eq(Tuple{Value{false, 5}}));
        ASSERT_THAT(got.value().tuples.size(), Eq(2));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, FilterMany) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users WHERE users.age > 10;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(got.value().tuples[0], Eq(Tuple{Value{false, 1}}));
        ASSERT_THAT(got.value().tuples.size(), Eq(15));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, CrossJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM users, books WHERE users.age < 10;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

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
        ASSERT_THAT(got.value().tuples[0], Eq(Tuple{Value{false, 5}, Value{false, 1}, Value{false, 1}, Value{false, 55}}));
        ASSERT_THAT(got.value().tuples.size(), Eq(6));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, InnerJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM employees JOIN departments ON employees.department_id = departments.id;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

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
        ASSERT_THAT(got.value().tuples.size(), Eq(3));
        ASSERT_THAT(ToString(got.value()), Eq(ReadFromFile(kProjectDir+"/test/static/executor/expected_inner_join.txt")));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, LeftJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM employees LEFT OUTER JOIN departments ON employees.department_id = departments.id;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

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

TYPED_TEST_P(ExecutorTest, RightJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT * FROM employees RIGHT JOIN departments ON employees.department_id = departments.id;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

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
        ASSERT_THAT(got.value().tuples.size(), Eq(5));
        ASSERT_THAT(ToString(got.value()), Eq(ReadFromFile(kProjectDir+"/test/static/executor/expected_right_join.txt")));
      });

  pool.join();
}

TYPED_TEST_P(ExecutorTest, ComplexJoin) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT departments.id*2, employees.id+1 FROM employees RIGHT JOIN departments ON employees.department_id = departments.id AND departments.id > 3 AND departments.id*2*2/2 < 30;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value());
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<TypeParam> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{
                                                {"", "", Type::kInt},
                                                {"", "", Type::kInt},
                                            }));
        ASSERT_THAT(got.value().tuples.size(), Eq(5));
        ASSERT_THAT(ToString(got.value()), Eq(ReadFromFile(kProjectDir+"/test/static/executor/expected_complex_join.txt")));
      });

  pool.join();
}

TEST(ExecutorTest, InnerJoinLargeRhsBug) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        Expression qual{BinaryExpression{
            .lhs = std::make_shared<Expression>(Attribute{"employees", "id"}),
            .binop = BinaryOp::kGt,
            .rhs = std::make_shared<Expression>(IntConst{0}),
        }};
        PhysicalPlanNode op = NestedLoopJoin{
            .lhs = std::make_shared<PhysicalPlanNode>(SeqScan{.table = "employees"}),
            .rhs = std::make_shared<PhysicalPlanNode>(SeqScan{.table = "departments_4000"}),
            .type = JoinType::kInner,
            .qual = std::move(qual),
        };
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);
        ASSERT_THAT(got.value().tuples.size(), Eq(44000u));
      });
  pool.join();
}

REGISTER_TYPED_TEST_SUITE_P(ExecutorTest, Projection, Filter, FilterMany, CrossJoin, InnerJoin, LeftJoin, RightJoin, ComplexJoin);
using ExecutorTypes = ::testing::Types<InterpretedExpressionExecutor, JitCompiledExpressionExecutor>;
INSTANTIATE_TYPED_TEST_SUITE_P(TypedExecutorTest, ExecutorTest, ExecutorTypes);

}  // namespace stewkk::sql
