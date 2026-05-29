#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <string_view>

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
      return SeqScan{.table = t.name, .alias = t.alias};
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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

TEST(ExecutorTest, StringFilterProjectionAndCsvQuotes) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT m.region, m.note FROM markets AS m WHERE m.region = 'AMERICA';"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"m", "region", Type::kString}, {"m", "note", Type::kString}}));
        ASSERT_THAT(got.value().tuples.size(), Eq(2));
        ASSERT_THAT(GetInternedString(got.value().tuples[0][0].value.string_id), Eq("AMERICA"));
        ASSERT_THAT(GetInternedString(got.value().tuples[0][1].value.string_id), Eq("North, South"));
        ASSERT_THAT(GetInternedString(got.value().tuples[1][1].value.string_id), Eq("Fast lane"));
      });

  ctx.run();
}

TEST(ExecutorTest, StringInFilter) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT m.id FROM markets AS m WHERE m.region IN ('AMERICA', 'ASIA');"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{{"m", "id", Type::kInt}}));
        ASSERT_THAT(got.value().tuples, Eq(Tuples{{Value{false, 1}}, {Value{false, 3}}}));
      });

  ctx.run();
}

TEST(ExecutorTest, StringNotInFilter) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT m.id FROM markets AS m WHERE m.region NOT IN ('AMERICA', 'ASIA');"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{{"m", "id", Type::kInt}}));
        ASSERT_THAT(got.value().tuples, Eq(Tuples{{Value{false, 2}}}));
      });

  ctx.run();
}

TEST(ExecutorTest, IntegerBetweenFilter) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users WHERE users.age BETWEEN 5 AND 11;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);

        auto got = co_await executor.Execute(op);

        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes, Eq(AttributesInfo{{"users", "id", Type::kInt}}));
        ASSERT_THAT(got.value().tuples, Eq(Tuples{{Value{false, 4}}, {Value{false, 6}}}));
      });

  ctx.run();
}

TEST(ExecutorTest, InNullSemantics) {
  AttributesInfo attrs{{"t", "x", Type::kInt}};
  Tuple tuple{Value{false, 2}};

  Expression positive = InExpression{
      std::make_shared<Expression>(Attribute{"t", "x"}),
      {IntConst{1}, Literal::kNull},
      false,
  };
  Expression negative = InExpression{
      std::make_shared<Expression>(Attribute{"t", "x"}),
      {IntConst{1}, Literal::kNull},
      true,
  };
  Expression matched = InExpression{
      std::make_shared<Expression>(Attribute{"t", "x"}),
      {IntConst{2}, Literal::kNull},
      false,
  };
  Expression null_lhs = InExpression{
      std::make_shared<Expression>(Literal::kNull),
      {IntConst{1}, IntConst{2}},
      false,
  };

  ASSERT_THAT(CalcExpression(tuple, attrs, positive).is_null, Eq(true));
  ASSERT_THAT(CalcExpression(tuple, attrs, negative).is_null, Eq(true));
  ASSERT_THAT(CalcExpression(tuple, attrs, matched), Eq(Value{false, true}));
  ASSERT_THAT(CalcExpression(tuple, attrs, null_lhs).is_null, Eq(true));
}

TEST(ExecutorTest, JitRejectsStringExpressions) {
  boost::asio::io_context ctx;
  bool rejected = false;
  boost::asio::co_spawn(
      ctx,
      [&rejected]() -> boost::asio::awaitable<void> {
        std::stringstream s{"SELECT m.id FROM markets AS m WHERE m.region = 'AMERICA';"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<JitCompiledExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);

        try {
          (void) co_await executor.Execute(op);
        } catch (const std::logic_error& e) {
          rejected = std::string_view{e.what()}.find("string expressions are not supported by JIT")
              != std::string_view::npos;
        }
      }(),
      [](std::exception_ptr p) {
        if (p) std::rethrow_exception(p);
      });

  ctx.run();
  ASSERT_THAT(rejected, Eq(true));
}

TEST(ExecutorTest, JitRejectsInExpressions) {
  boost::asio::io_context ctx;
  bool rejected = false;
  boost::asio::co_spawn(
      ctx,
      [&rejected]() -> boost::asio::awaitable<void> {
        std::stringstream s{"SELECT users.id FROM users WHERE users.age IN (1, 2);"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<JitCompiledExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);

        try {
          (void) co_await executor.Execute(op);
        } catch (const std::logic_error& e) {
          rejected = std::string_view{e.what()}.find("IN expressions are not supported by JIT")
              != std::string_view::npos;
        }
      }(),
      [](std::exception_ptr p) {
        if (p) std::rethrow_exception(p);
      });

  ctx.run();
  ASSERT_THAT(rejected, Eq(true));
}

TYPED_TEST_P(ExecutorTest, Projection) {
  boost::asio::thread_pool pool{4};
  boost::asio::co_spawn(
      pool,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.id FROM users;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
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

TEST(ExecutorTest, ScalarCountStar) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT COUNT(*) FROM users;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);
        auto got = co_await executor.Execute(op);
        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);
        ASSERT_THAT(got.value().tuples.size(), Eq(1u));
        ASSERT_THAT(got.value().tuples[0][0], Eq(Value{false, {.int_value = 17}}));
      });
  ctx.run();
}

TEST(ExecutorTest, ScalarSum) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT SUM(users.age) FROM users;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);
        auto got = co_await executor.Execute(op);
        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);
        ASSERT_THAT(got.value().tuples.size(), Eq(1u));
        ASSERT_THAT(got.value().tuples[0][0], Eq(Value{false, {.int_value = 1182}}));
      });
  ctx.run();
}

TEST(ExecutorTest, GroupByCountStar) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.age, COUNT(*) FROM users GROUP BY users.age;"};
        PhysicalPlanNode op = ToPhysicalPlan(GetAST(s).value().op);
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);
        auto got = co_await executor.Execute(op);
        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);
        // 11 distinct age values in users.csv
        ASSERT_THAT(got.value().tuples.size(), Eq(11u));
        // Sum of counts must equal total rows (17)
        int64_t total = 0;
        for (const auto& t : got.value().tuples) {
          total += t[1].value.int_value;
        }
        ASSERT_THAT(total, Eq(17));
      });
  ctx.run();
}

}  // namespace stewkk::sql
