#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>
#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>
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

} // namespace

const static std::string kProjectDir = std::getenv("PWD");

namespace {

std::string ReadFromFile(std::filesystem::path path) {
  std::ifstream f{path};
  std::ostringstream stream;
  stream << f.rdbuf();
  return stream.str();
}

boost::asio::awaitable<Result<Relation>> RunIndexSeekTestQuery(std::string dir) {
  PhysicalPlanNode op = IndexSeek{
      "users",
      std::nullopt,
      BinaryExpression{
          std::make_shared<Expression>(Attribute{"users", "id"}),
          BinaryOp::kGe,
          std::make_shared<Expression>(IntConst{8}),
      },
  };
  CsvDirSequentialScanner seq_scan{dir};
  CsvDirIndexedScanner index_scan{dir};
  Executor executor(std::move(seq_scan), std::move(index_scan),
                    co_await boost::asio::this_coro::executor);

  co_return co_await executor.Execute(op);
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

TEST(ExecutorTest, IndexSeekBuildsAndUsesSortedIntIndex) {
  auto dir = std::filesystem::temp_directory_path() / "iu9_sql_index_seek_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  {
    std::ofstream csv{dir / "users.csv"};
    csv << "id:int,age:int\n"
        << "10,22\n"
        << "1,33\n"
        << "8,64\n"
        << "8,70\n";
    std::ofstream meta{dir / "indexes.meta"};
    meta << "users id sorted users.id.sorted.idx\n";
  }

  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      RunIndexSeekTestQuery(dir.string()),
      [dir](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);

        ASSERT_THAT(got.value().attributes,
                    Eq(AttributesInfo{{"users", "id", Type::kInt}, {"users", "age", Type::kInt}}));
        ASSERT_THAT(got.value().tuples,
                    Eq(Tuples{
                        Tuple{Value{false, 8}, Value{false, 64}},
                        Tuple{Value{false, 8}, Value{false, 70}},
                        Tuple{Value{false, 10}, Value{false, 22}},
                    }));
        ASSERT_TRUE(std::filesystem::exists(dir / "users.id.sorted.idx"));
        std::filesystem::remove_all(dir);
      });

  ctx.run();
}

TEST(ExecutorTest, OptimizedIndexSeekPlanExecutes) {
  auto dir = std::filesystem::temp_directory_path() / "iu9_sql_optimized_index_seek_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  {
    std::ofstream csv{dir / "users.csv"};
    csv << "id:int,age:int\n"
        << "10,22\n"
        << "1,33\n"
        << "8,64\n"
        << "8,70\n";
    std::ofstream meta{dir / "indexes.meta"};
    meta << "users id sorted users.id.sorted.idx\n";
  }

  std::stringstream sql{"SELECT * FROM users WHERE users.id >= 8 AND users.age < 70;"};
  auto parsed = GetAST(sql).value();
  Optimizer optimizer(parsed.op, MakeMainRules(LoadIndexCatalogFromCsvDir(dir)),
                      CardinalityEstimates({{"users", 4}}),
                      LoadSchemaFromCsvDir(dir));
  auto plan = optimizer.Optimize();

  boost::asio::io_context ctx;
  CsvDirSequentialScanner seq_scan{dir.string()};
  CsvDirIndexedScanner index_scan{dir.string()};
  Executor executor(std::move(seq_scan), std::move(index_scan), ctx.get_executor());
  auto fut = boost::asio::co_spawn(ctx, executor.Execute(plan), boost::asio::use_future);
  ctx.run();
  auto got = fut.get();

  ASSERT_THAT(got.value().attributes,
              Eq(AttributesInfo{{"users", "id", Type::kInt}, {"users", "age", Type::kInt}}));
  ASSERT_THAT(got.value().tuples,
              Eq(Tuples{
                  Tuple{Value{false, 8}, Value{false, 64}},
                  Tuple{Value{false, 10}, Value{false, 22}},
              }));
  std::filesystem::remove_all(dir);
}

TEST(ExecutorTest, MergeJoinFullOuterWithDuplicatesAndNulls) {
  auto dir = std::filesystem::temp_directory_path() / "iu9_sql_merge_join_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  {
    std::ofstream a{dir / "a.csv"};
    a << "id:int,v:int\n"
      << "1,10\n"
      << "1,11\n"
      << "2,20\n"
      << "NULL,99\n";
    std::ofstream b{dir / "b.csv"};
    b << "id:int,w:int\n"
      << "1,100\n"
      << "1,101\n"
      << "3,300\n"
      << "NULL,999\n";
  }

  Expression qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"a", "id"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"b", "id"}),
  };
  PhysicalPlanNode plan = MergeJoin{
      std::make_shared<PhysicalPlanNode>(PhysicalSort{
          std::make_shared<PhysicalPlanNode>(SeqScan{"a"}),
          SortOrder{{SortKey{"a", "id", Direction::kAsc}}}}),
      std::make_shared<PhysicalPlanNode>(PhysicalSort{
          std::make_shared<PhysicalPlanNode>(SeqScan{"b"}),
          SortOrder{{SortKey{"b", "id", Direction::kAsc}}}}),
      JoinType::kFull,
      qual,
  };

  boost::asio::io_context ctx;
  CsvDirSequentialScanner seq_scan{dir.string()};
  Executor executor(std::move(seq_scan), ctx.get_executor());
  auto fut = boost::asio::co_spawn(ctx, executor.Execute(plan), boost::asio::use_future);
  ctx.run();
  auto got = fut.get();

  ASSERT_THAT(got.value().tuples,
              Eq(Tuples{
                  Tuple{Value{false, 1}, Value{false, 10}, Value{false, 1}, Value{false, 100}},
                  Tuple{Value{false, 1}, Value{false, 10}, Value{false, 1}, Value{false, 101}},
                  Tuple{Value{false, 1}, Value{false, 11}, Value{false, 1}, Value{false, 100}},
                  Tuple{Value{false, 1}, Value{false, 11}, Value{false, 1}, Value{false, 101}},
                  Tuple{Value{false, 2}, Value{false, 20}, Value{true}, Value{true}},
                  Tuple{Value{true}, Value{false, 99}, Value{true}, Value{true}},
                  Tuple{Value{true}, Value{true}, Value{false, 3}, Value{false, 300}},
                  Tuple{Value{true}, Value{true}, Value{true}, Value{false, 999}},
              }));
  std::filesystem::remove_all(dir);
}

TEST(ExecutorTest, MergeJoinStringKeys) {
  auto dir = std::filesystem::temp_directory_path() / "iu9_sql_merge_join_string_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  {
    std::ofstream lhs{dir / "lhs.csv"};
    lhs << "name:string,v:int\n"
        << "alpha,1\n"
        << "beta,2\n";
    std::ofstream rhs{dir / "rhs.csv"};
    rhs << "name:string,w:int\n"
        << "alpha,10\n"
        << "gamma,30\n";
  }

  Expression qual = BinaryExpression{
      std::make_shared<Expression>(Attribute{"lhs", "name"}),
      BinaryOp::kEq,
      std::make_shared<Expression>(Attribute{"rhs", "name"}),
  };
  PhysicalPlanNode plan = MergeJoin{
      std::make_shared<PhysicalPlanNode>(PhysicalSort{
          std::make_shared<PhysicalPlanNode>(SeqScan{"lhs"}),
          SortOrder{{SortKey{"lhs", "name", Direction::kAsc}}}}),
      std::make_shared<PhysicalPlanNode>(PhysicalSort{
          std::make_shared<PhysicalPlanNode>(SeqScan{"rhs"}),
          SortOrder{{SortKey{"rhs", "name", Direction::kAsc}}}}),
      JoinType::kInner,
      qual,
  };

  boost::asio::io_context ctx;
  CsvDirSequentialScanner seq_scan{dir.string()};
  Executor executor(std::move(seq_scan), ctx.get_executor());
  auto fut = boost::asio::co_spawn(ctx, executor.Execute(plan), boost::asio::use_future);
  ctx.run();
  auto got = fut.get();

  ASSERT_THAT(got.value().tuples.size(), Eq(1u));
  ASSERT_THAT(GetInternedString(got.value().tuples[0][0].value.string_id), Eq("alpha"));
  ASSERT_THAT(GetInternedString(got.value().tuples[0][2].value.string_id), Eq("alpha"));
  ASSERT_THAT(got.value().tuples[0][1], Eq(Value{false, 1}));
  ASSERT_THAT(got.value().tuples[0][3], Eq(Value{false, 10}));
  std::filesystem::remove_all(dir);
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

TEST(ExecutorTest, StringOrderedFilter) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT m.id FROM markets AS m WHERE m.region >= 'AMERICA';"};
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
        ASSERT_THAT(got.value().tuples, Eq(Tuples{{Value{false, 1}}, {Value{false, 2}}, {Value{false, 3}}}));
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

TEST(ExecutorTest, StringOrderedComparison) {
  AttributesInfo attrs{{"t", "x", Type::kString}};
  Tuple tuple{Value{false, {.string_id = InternString("MFGR#2223")}}};

  Expression lower = BinaryExpression{
      std::make_shared<Expression>(Attribute{"t", "x"}),
      BinaryOp::kGe,
      std::make_shared<Expression>(StringConst{"MFGR#2221"}),
  };
  Expression upper = BinaryExpression{
      std::make_shared<Expression>(Attribute{"t", "x"}),
      BinaryOp::kLe,
      std::make_shared<Expression>(StringConst{"MFGR#2228"}),
  };

  ASSERT_THAT(CalcExpression(tuple, attrs, lower),
              Eq(Value{false, {.bool_value = true}}));
  ASSERT_THAT(CalcExpression(tuple, attrs, upper),
              Eq(Value{false, {.bool_value = true}}));
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
       
        ASSERT_THAT(got.value().tuples.size(), Eq(11u));
       
        int64_t total = 0;
        for (const auto& t : got.value().tuples) {
          total += t[1].value.int_value;
        }
        ASSERT_THAT(total, Eq(17));
      });
  ctx.run();
}

TEST(ExecutorTest, StreamGroupByCountStar) {
  boost::asio::io_context ctx;
  boost::asio::co_spawn(
      ctx,
      []() -> boost::asio::awaitable<Result<Relation>> {
        std::stringstream s{"SELECT users.age, COUNT(*) FROM users GROUP BY users.age;"};
        auto ast = GetAST(s).value().op;
        const auto& projection = std::get<Projection>(ast);
        const auto& agg = std::get<Aggregation>(*projection.source);
        PhysicalPlanNode op = PhysicalStreamAggregation{
            .source = std::make_shared<PhysicalPlanNode>(PhysicalSort{
                .source = std::make_shared<PhysicalPlanNode>(ToPhysicalPlan(*agg.source)),
                .keys = SortOrder{{SortKey{"users", "age", Direction::kAsc}}},
            }),
            .group_by = agg.group_by,
            .aggregates = agg.aggregates,
        };
        CsvDirSequentialScanner seq_scan{kProjectDir + "/test/static/executor/test_data"};
        Executor<InterpretedExpressionExecutor> executor(
            std::move(seq_scan), co_await boost::asio::this_coro::executor);
        auto got = co_await executor.Execute(op);
        co_return got;
      }(),
      [](std::exception_ptr p, Result<Relation> got) {
        if (p) std::rethrow_exception(p);
        ASSERT_THAT(got.value().tuples.size(), Eq(11u));

        int64_t total = 0;
        for (const auto& t : got.value().tuples) {
          total += t[1].value.int_value;
        }
        ASSERT_THAT(total, Eq(17));
      });
  ctx.run();
}

}  // namespace stewkk::sql
