#include <benchmark/benchmark.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {
namespace {

struct BenchTable {
  AttributesInfo attrs;
  Tuples tuples;
};

using BenchTables = std::unordered_map<std::string, BenchTable>;

Value IntValue(int64_t value) {
  return Value{false, {.int_value = value}};
}

Attribute Attr(std::string table, std::string name) {
  return Attribute{std::move(table), std::move(name)};
}

Expression AttrExpr(std::string table, std::string name) {
  return Expression{Attr(std::move(table), std::move(name))};
}

Expression IntExpr(int64_t value) {
  return Expression{IntConst{value}};
}

Expression Binary(Expression lhs, BinaryOp op, Expression rhs) {
  return Expression{BinaryExpression{
      .lhs = std::make_shared<Expression>(std::move(lhs)),
      .binop = op,
      .rhs = std::make_shared<Expression>(std::move(rhs)),
  }};
}

Expression CountStar() {
  return Expression{AggregateExpression{
      .function = AggregateFunction::kCount,
      .argument = nullptr,
      .is_star = true,
  }};
}

PhysicalPlanNode Scan(std::string table) {
  return PhysicalPlanNode{SeqScan{.table = std::move(table), .alias = std::nullopt}};
}

std::shared_ptr<PhysicalPlanNode> PlanPtr(PhysicalPlanNode plan) {
  return std::make_shared<PhysicalPlanNode>(std::move(plan));
}

BenchTable MakeTable(std::string table, int64_t rows) {
  BenchTable result;
  result.attrs = {
      AttributeInfo{table, "id", Type::kInt},
      AttributeInfo{table, "k", Type::kInt},
      AttributeInfo{table, "v", Type::kInt},
  };
  result.tuples.reserve(static_cast<size_t>(rows));
  for (int64_t i = 0; i < rows; ++i) {
    result.tuples.push_back(Tuple{
        IntValue(i),
        IntValue(i),
        IntValue(i % 1024),
    });
  }
  return result;
}

std::shared_ptr<const BenchTables> MakeTables(int64_t rows_t, int64_t rows_u = 0) {
  auto tables = std::make_shared<BenchTables>();
  tables->emplace("t", MakeTable("t", rows_t));
  tables->emplace("u", MakeTable("u", rows_u));
  return tables;
}

std::shared_ptr<const BenchTables> MakeSortTables(int64_t rows) {
  auto tables = std::make_shared<BenchTables>();
  auto table = MakeTable("t", rows);
  std::mt19937 generator{0};
  std::shuffle(table.tuples.begin(), table.tuples.end(), generator);
  tables->emplace("t", std::move(table));
  return tables;
}

class InMemorySequentialScanner {
public:
  explicit InMemorySequentialScanner(std::shared_ptr<const BenchTables> tables)
      : tables_(std::move(tables)) {}

  boost::asio::awaitable<Result<>> operator()(const std::string& table_name,
                                              const std::string& output_table_name,
                                              AttributesInfoChannel& attrs_chan,
                                              TuplesChannel& tuples_chan) const {
    auto it = tables_->find(table_name);
    if (it == tables_->end()) {
      throw std::runtime_error{"unknown benchmark table: " + table_name};
    }

    auto attrs = it->second.attrs;
    for (auto& attr : attrs) {
      attr.table = output_table_name;
    }
    co_await attrs_chan.async_send(boost::system::error_code{}, std::move(attrs),
                                   boost::asio::use_awaitable);
    attrs_chan.close();

    Tuples buf;
    buf.reserve(kBufSize);
    for (const auto& tuple : it->second.tuples) {
      buf.push_back(tuple);
      if (buf.size() == kBufSize) {
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                        boost::asio::use_awaitable);
        buf.clear();
        buf.reserve(kBufSize);
      }
    }
    if (!buf.empty()) {
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                      boost::asio::use_awaitable);
    }
    tuples_chan.close();
    co_return Ok();
  }

private:
  std::shared_ptr<const BenchTables> tables_;
};

Relation RunPlan(const PhysicalPlanNode& plan, std::shared_ptr<const BenchTables> tables) {
  boost::asio::io_context ctx;
  Executor<InterpretedExpressionExecutor> executor(InMemorySequentialScanner{std::move(tables)},
                                                   ctx.get_executor());
  auto fut = boost::asio::co_spawn(ctx, executor.Execute(plan), boost::asio::use_future);
  ctx.run();
  auto result = fut.get();
  if (!result.has_value()) {
    throw std::runtime_error{What(result.error())};
  }
  return std::move(result).value();
}

int64_t SortModelCost(int64_t rows) {
  return 11 * (rows > 1 ? rows * static_cast<int64_t>(std::bit_width(static_cast<uint64_t>(rows))) : rows);
}

int64_t FindSortRowsForTargetCost(int64_t target_cost) {
  int64_t best_rows = 1;
  for (int64_t rows = 2; SortModelCost(rows) <= 2 * target_cost; ++rows) {
    if (std::abs(SortModelCost(rows) - target_cost)
        < std::abs(SortModelCost(best_rows) - target_cost)) {
      best_rows = rows;
    }
  }
  return best_rows;
}

int64_t DivideRounded(int64_t value, int64_t divisor) {
  return std::max<int64_t>(1, (value + divisor / 2) / divisor);
}

int64_t SqrtRounded(int64_t value) {
  const auto floor = static_cast<int64_t>(std::sqrt(value));
  return value - floor * floor < (floor + 1) * (floor + 1) - value ? floor : floor + 1;
}

void SetUnaryCounters(benchmark::State& state, int64_t rows, int64_t model_cost,
                      int64_t plan_cost, int64_t output_rows) {
  state.counters["rows"] = static_cast<double>(rows);
  state.counters["model_cost"] = static_cast<double>(model_cost);
  state.counters["plan_cost"] = static_cast<double>(plan_cost);
  state.counters["output_rows"] = static_cast<double>(output_rows);
}

void SetBinaryCounters(benchmark::State& state, int64_t lhs_rows, int64_t rhs_rows,
                       int64_t model_cost, int64_t plan_cost, int64_t output_rows) {
  state.counters["lhs_rows"] = static_cast<double>(lhs_rows);
  state.counters["rhs_rows"] = static_cast<double>(rhs_rows);
  state.counters["model_cost"] = static_cast<double>(model_cost);
  state.counters["plan_cost"] = static_cast<double>(plan_cost);
  state.counters["output_rows"] = static_cast<double>(output_rows);
}

void SuppressClog() {
  static std::ofstream nullstream("/dev/null");
  std::clog.rdbuf(nullstream.rdbuf());
}

void BM_OperatorSeqScan(benchmark::State& state) {
  const int64_t rows = state.range(0);
  auto tables = MakeTables(rows);
  auto plan = Scan("t");

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetUnaryCounters(state, rows, 100 * rows, 100 * rows, rows);
}

void BM_OperatorFilter(benchmark::State& state) {
  const int64_t rows = state.range(0);
  constexpr int64_t kThreshold = 512;
  auto tables = MakeTables(rows);
  auto plan = PhysicalPlanNode{PhysicalFilter{
      .source = PlanPtr(Scan("t")),
      .predicate = Binary(AttrExpr("t", "v"), BinaryOp::kLt, IntExpr(kThreshold)),
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  const int64_t full_cycles = rows / 1024;
  const int64_t tail = rows % 1024;
  const int64_t output_rows = full_cycles * kThreshold + std::min<int64_t>(tail, kThreshold);
  SetUnaryCounters(state, rows, 100 * rows, 200 * rows, output_rows);
}

void BM_OperatorProjection(benchmark::State& state) {
  const int64_t rows = state.range(0);
  auto tables = MakeTables(rows);
  auto plan = PhysicalPlanNode{PhysicalProjection{
      .source = PlanPtr(Scan("t")),
      .expressions = {
          AttrExpr("t", "id"),
          Binary(AttrExpr("t", "v"), BinaryOp::kPlus, IntExpr(1)),
      },
      .aliases = {std::nullopt, std::nullopt},
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetUnaryCounters(state, rows, 22 * rows, 122 * rows, rows);
}

void BM_OperatorSort(benchmark::State& state) {
  const int64_t rows = state.range(0);
  auto tables = MakeSortTables(rows);
  auto plan = PhysicalPlanNode{PhysicalSort{
      .source = PlanPtr(Scan("t")),
      .keys = SortOrder{{SortKey{.table = "t", .column = "id", .dir = Direction::kAsc}}},
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetUnaryCounters(state, rows, SortModelCost(rows), SortModelCost(rows) + 100 * rows, rows);
}

void BM_OperatorAggregation(benchmark::State& state) {
  const int64_t rows = state.range(0);
  auto tables = MakeTables(rows);
  auto plan = PhysicalPlanNode{PhysicalAggregation{
      .source = PlanPtr(Scan("t")),
      .group_by = {AttrExpr("t", "k")},
      .aggregates = {CountStar()},
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetUnaryCounters(state, rows, 510 * rows, 610 * rows, rows);
}

void BM_OperatorNestedLoopJoin(benchmark::State& state) {
  const int64_t lhs_rows = state.range(0);
  const int64_t rhs_rows = state.range(1);
  auto tables = MakeTables(lhs_rows, rhs_rows);
  auto plan = PhysicalPlanNode{NestedLoopJoin{
      .lhs = PlanPtr(Scan("t")),
      .rhs = PlanPtr(Scan("u")),
      .type = JoinType::kInner,
      .qual = Binary(AttrExpr("t", "k"), BinaryOp::kEq, AttrExpr("u", "k")),
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetBinaryCounters(state, lhs_rows, rhs_rows, 70 * lhs_rows * rhs_rows,
                    70 * lhs_rows * rhs_rows + 100 * (lhs_rows + rhs_rows),
                    std::min(lhs_rows, rhs_rows));
}

void BM_OperatorNestedLoopCrossJoin(benchmark::State& state) {
  const int64_t lhs_rows = state.range(0);
  const int64_t rhs_rows = state.range(1);
  auto tables = MakeTables(lhs_rows, rhs_rows);
  auto plan = PhysicalPlanNode{NestedLoopCrossJoin{
      .lhs = PlanPtr(Scan("t")),
      .rhs = PlanPtr(Scan("u")),
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetBinaryCounters(state, lhs_rows, rhs_rows, 104 * lhs_rows * rhs_rows,
                    104 * lhs_rows * rhs_rows + 100 * (lhs_rows + rhs_rows),
                    lhs_rows * rhs_rows);
}

void BM_OperatorHashJoin(benchmark::State& state) {
  const int64_t lhs_rows = state.range(0);
  const int64_t rhs_rows = state.range(1);
  auto tables = MakeTables(lhs_rows, rhs_rows);
  auto plan = PhysicalPlanNode{HashJoin{
      .lhs = PlanPtr(Scan("t")),
      .rhs = PlanPtr(Scan("u")),
      .type = JoinType::kInner,
      .qual = Binary(AttrExpr("t", "k"), BinaryOp::kEq, AttrExpr("u", "k")),
  }};

  SuppressClog();

  benchmark::DoNotOptimize(RunPlan(plan, tables));
  for (auto _ : state) {
    benchmark::DoNotOptimize(RunPlan(plan, tables));
  }
  SetBinaryCounters(state, lhs_rows, rhs_rows, 69 * (lhs_rows + rhs_rows),
                    169 * (lhs_rows + rhs_rows),
                    std::min(lhs_rows, rhs_rows));
}

void RegisterUnary(void (*benchmark_fn)(benchmark::State&), const char* name) {
  for (int64_t rows : {1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144}) {
    benchmark::RegisterBenchmark(name, benchmark_fn)->Arg(rows)->UseRealTime();
  }
}

void RegisterCostMatched(int64_t target_cost) {
  const auto prefix = "OperatorCostMatched/target_cost:" + std::to_string(target_cost) + "/";
  const auto register_unary = [&](const char* name, void (*benchmark_fn)(benchmark::State&),
                                  int64_t rows) {
    benchmark::RegisterBenchmark((prefix + name).c_str(), benchmark_fn)->Arg(rows)->UseRealTime();
  };
  const auto register_binary = [&](const char* name, void (*benchmark_fn)(benchmark::State&),
                                   int64_t rows) {
    benchmark::RegisterBenchmark((prefix + name).c_str(), benchmark_fn)
        ->Args({rows, rows})
        ->UseRealTime();
  };

  register_unary("SeqScan", BM_OperatorSeqScan, DivideRounded(target_cost, 100));
  register_unary("Filter", BM_OperatorFilter, DivideRounded(target_cost, 100));
  register_unary("Projection", BM_OperatorProjection, DivideRounded(target_cost, 22));
  register_unary("Sort", BM_OperatorSort, FindSortRowsForTargetCost(target_cost));
  register_unary("Aggregation", BM_OperatorAggregation, DivideRounded(target_cost, 510));
  register_binary("HashJoin", BM_OperatorHashJoin, DivideRounded(target_cost, 2 * 69));
  register_binary("NestedLoopJoin", BM_OperatorNestedLoopJoin,
                  SqrtRounded(DivideRounded(target_cost, 70)));
  register_binary("NestedLoopCrossJoin", BM_OperatorNestedLoopCrossJoin,
                  SqrtRounded(DivideRounded(target_cost, 104)));
}

struct OperatorCostRegistration {
  OperatorCostRegistration() {
    RegisterUnary(BM_OperatorSeqScan, "OperatorCost/SeqScan");
    RegisterUnary(BM_OperatorFilter, "OperatorCost/Filter");
    RegisterUnary(BM_OperatorProjection, "OperatorCost/Projection");
    RegisterUnary(BM_OperatorSort, "OperatorCost/Sort");
    RegisterUnary(BM_OperatorAggregation, "OperatorCost/Aggregation");

    for (auto size : {1024, 2048, 4096, 8192, 16384, 32768, 65536}) {
      benchmark::RegisterBenchmark("OperatorCost/HashJoin", BM_OperatorHashJoin)
          ->Args({size, size})
          ->UseRealTime();
    }
    for (auto size : {64, 128, 256, 512, 1024, 2048}) {
      benchmark::RegisterBenchmark("OperatorCost/NestedLoopJoin", BM_OperatorNestedLoopJoin)
          ->Args({size, size})
          ->UseRealTime();
    }
    for (auto size : {32, 64, 128, 256, 512, 1024}) {
      benchmark::RegisterBenchmark("OperatorCost/NestedLoopCrossJoin",
                                   BM_OperatorNestedLoopCrossJoin)
          ->Args({size, size})
          ->UseRealTime();
    }

    for (auto target_cost : {640000, 1000000, 3240000, 6760000, 12250000, 26010000}) {
      RegisterCostMatched(target_cost);
    }
  }
};

const OperatorCostRegistration kRegisterOperatorCost;

}  // namespace
}  // namespace stewkk::sql
