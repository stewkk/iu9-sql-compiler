#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <future>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>

#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/logic/executor/plan_serializer.hpp>
#include <stewkk/sql/logic/executor/sequential_scan.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>
#include <stewkk/sql/logic/optimizer/reachability.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>
#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/utils/output_dot_plans.hpp>

namespace stewkk::sql {

namespace {

constexpr int kOk = 0;
constexpr int kUsage = 64;
constexpr int kParseError = 1;
constexpr int kOptimizerError = 2;
constexpr int kRuntimeError = 3;
constexpr int kNotReachable = 4;

struct Args {
  std::string data_dir;
  bool print_plan = false;
  bool print_ast = false;
  bool jit = false;
  bool stats = false;
  std::string check_reachable_path;
};

Args ParseArgs(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--data-dir") {
      if (i + 1 >= argc) {
        std::cerr << "--data-dir requires an argument\n";
        std::exit(kUsage);
      }
      args.data_dir = argv[++i];
    } else if (a == "--print-plan") {
      args.print_plan = true;
    } else if (a == "--print-ast") {
      args.print_ast = true;
    } else if (a == "--jit") {
      args.jit = true;
    } else if (a == "--stats") {
      args.stats = true;
    } else if (a == "--check-reachable") {
      if (i + 1 >= argc) {
        std::cerr << "--check-reachable requires a plan file path\n";
        std::exit(kUsage);
      }
      args.check_reachable_path = argv[++i];
    } else {
      std::cerr << "unknown argument: " << a << "\n";
      std::exit(kUsage);
    }
  }
  if (args.data_dir.empty() && args.check_reachable_path.empty()) {
    std::cerr << "usage: sql --data-dir <csv_dir> [--print-plan] [--jit] [--stats] < query.sql\n"
              << "       sql --check-reachable <plan_file> < query.sql\n";
    std::exit(kUsage);
  }
  return args;
}

std::string SerializeAst(const Operator& op) {
  return std::visit([](auto&& node) -> std::string {
    using T = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<T, Table>) {
      return node.alias ? "(Table " + node.name + " AS " + *node.alias + ")"
                        : "(Table " + node.name + ")";
    } else if constexpr (std::is_same_v<T, Filter>) {
      return "(Filter " + ToString(node.expr) + " " + SerializeAst(*node.source) + ")";
    } else if constexpr (std::is_same_v<T, Projection>) {
      std::string exprs;
      for (size_t i = 0; i < node.expressions.size(); ++i) {
        exprs += " " + ToString(node.expressions[i]);
        if (i < node.aliases.size() && node.aliases[i]) {
          exprs += " AS " + *node.aliases[i];
        }
      }
      return "(Projection" + exprs + " " + SerializeAst(*node.source) + ")";
    } else if constexpr (std::is_same_v<T, Aggregation>) {
      std::string group_by;
      for (const auto& e : node.group_by) group_by += " " + ToString(e);
      std::string aggregates;
      for (const auto& e : node.aggregates) aggregates += " " + ToString(e);
      return "(Aggregation (GroupBy" + group_by + ") (Aggregates" + aggregates + ") "
             + SerializeAst(*node.source) + ")";
    } else if constexpr (std::is_same_v<T, CrossJoin>) {
      return "(CrossJoin " + SerializeAst(*node.lhs) + " " + SerializeAst(*node.rhs) + ")";
    } else if constexpr (std::is_same_v<T, Join>) {
      return "(Join " + ToString(node.type) + " " + ToString(node.qual)
             + " " + SerializeAst(*node.lhs) + " " + SerializeAst(*node.rhs) + ")";
    }
    return "?";
  }, op);
}

std::string ValueToString(const Value& v, const AttributeInfo& attr) {
  if (v.is_null) return "NULL";
  switch (attr.type) {
    case Type::kInt:
      return std::to_string(v.value.int_value);
    case Type::kBool:
      return v.value.bool_value ? "1" : "0";
    case Type::kString:
      return GetInternedString(v.value.string_id);
  }
  return "?";
}

std::string TypeName(Type t) {
  switch (t) {
    case Type::kInt:
      return "int";
    case Type::kBool:
      return "bool";
    case Type::kString:
      return "string";
  }
  return "?";
}

void PrintRelation(const Relation& rel, bool preserve_order) {
  std::ostringstream header;
  for (size_t i = 0; i < rel.attributes.size(); ++i) {
    const auto& a = rel.attributes[i];
    if (i) header << '\t';
    if (!a.table.empty() || !a.name.empty()) {
      header << a.table << '.' << a.name;
    }
    header << ':' << TypeName(a.type);
  }

  std::vector<std::string> rows;
  rows.reserve(rel.tuples.size());
  for (const auto& tuple : rel.tuples) {
    std::ostringstream line;
    for (size_t i = 0; i < tuple.size(); ++i) {
      if (i) line << '\t';
      line << ValueToString(tuple[i], rel.attributes[i]);
    }
    rows.push_back(std::move(line).str());
  }
  if (!preserve_order) std::sort(rows.begin(), rows.end());

  std::cout << header.str() << '\n';
  for (const auto& r : rows) std::cout << r << '\n';
}

boost::asio::awaitable<Result<Relation>> RunQuery(const std::string& data_dir,
                                                  const PhysicalPlanNode& plan) {
  CsvDirSequentialScanner seq_scan{data_dir};
  CsvDirIndexedScanner index_scan{data_dir};
  Executor executor(std::move(seq_scan), std::move(index_scan), co_await boost::asio::this_coro::executor);
  co_return co_await executor.Execute(plan);
}

boost::asio::awaitable<Result<Relation>> RunQueryJit(const std::string& data_dir,
                                                     const PhysicalPlanNode& plan) {
  CsvDirSequentialScanner seq_scan{data_dir};
  Executor<CachedJitCompiledExpressionExecutor> executor(
      std::move(seq_scan), co_await boost::asio::this_coro::executor);
  co_return co_await executor.Execute(plan);
}

}  // namespace

}  // namespace stewkk::sql

int main(int argc, char** argv) {
  using namespace stewkk::sql;

  auto args = ParseArgs(argc, argv);

  std::ostringstream sql_buf;
  sql_buf << std::cin.rdbuf();
  std::stringstream sql_stream{sql_buf.str()};

  if (!args.check_reachable_path.empty()) {
    std::ifstream plan_in{args.check_reachable_path};
    if (!plan_in) {
      std::cerr << "cannot open plan file: " << args.check_reachable_path << "\n";
      return kUsage;
    }
    std::ostringstream plan_buf;
    plan_buf << plan_in.rdbuf();
    PhysicalPlanNode target;
    try {
      target = Deserialize(plan_buf.str());
    } catch (const std::exception& e) {
      std::cerr << "plan parse error: " << e.what() << "\n";
      return kParseError;
    }
    MatchResult mr;
    try {
      SchemaCatalog schema = args.data_dir.empty() ? SchemaCatalog{}
                                                    : LoadSchemaFromCsvDir(args.data_dir);
      mr = IsPlanReachable(sql_stream, target, {}, std::move(schema));
    } catch (const std::exception& e) {
      std::cerr << "reachability error: " << e.what() << "\n";
      return kOptimizerError;
    }
    if (mr.reachable) {
      std::cout << "REACHABLE\n";
      return kOk;
    }
    std::cout << "NOT REACHABLE: " << mr.mismatch << "\n";
    return kNotReachable;
  }

  auto parsed_result = GetAST(sql_stream);
  if (!parsed_result.has_value()) {
    std::cerr << "parse error: " << What(parsed_result.error()) << "\n";
    return kParseError;
  }
  auto parsed = std::move(parsed_result).value();

  if (args.print_ast) {
    std::cerr << SerializeAst(parsed.op) << "\n";
  }

  PhysicalPlanNode plan;
  PhysicalPlanNode naive_plan;
  std::int64_t plan_cost = 0;
  std::int64_t naive_plan_cost = 0;
  try {
    PropertySet required = parsed.required_order
        ? PropertySet{SortProperty{*parsed.required_order}}
        : PropertySet::Any();
    Optimizer optimizer(parsed.op, MakeMainRules(),
                        CardinalityEstimates{LoadTableSizesFromCsvDir(args.data_dir)},
                        LoadSchemaFromCsvDir(args.data_dir), std::move(required));
    plan = optimizer.Optimize();
    plan_cost = optimizer.GetBestCost();

    PropertySet naive_required = parsed.required_order
                                     ? PropertySet{SortProperty{*parsed.required_order}}
                                     : PropertySet::Any();
    Optimizer naive_optimizer(parsed.op, MakeNaiveRules(),
                              CardinalityEstimates{LoadTableSizesFromCsvDir(args.data_dir)},
                              LoadSchemaFromCsvDir(args.data_dir), std::move(naive_required));
    naive_plan = naive_optimizer.Optimize();
    naive_plan_cost = naive_optimizer.GetBestCost();
  } catch (const std::exception& e) {
    std::cerr << "optimizer error: " << e.what() << "\n";
    return kOptimizerError;
  }

  OutputDot(plan, naive_plan);

  if (args.print_plan) {
    std::cerr << Serialize(plan) << "\n";
  }

  Result<Relation> result;
  std::int64_t exec_us = 0;
  try {
    const auto exec_started = std::chrono::steady_clock::now();
    boost::asio::io_context ctx;
    boost::asio::any_io_executor exec = ctx.get_executor();
    auto fut = args.jit
        ? boost::asio::co_spawn(exec, RunQueryJit(args.data_dir, plan),
                                boost::asio::use_future)
        : boost::asio::co_spawn(exec, RunQuery(args.data_dir, plan),
                                boost::asio::use_future);
    ctx.run();
    result = fut.get();
    exec_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - exec_started).count();
  } catch (const std::exception& e) {
    std::cerr << "runtime error: " << e.what() << "\n";
    return kRuntimeError;
  }

  if (!result.has_value()) {
    std::cerr << "runtime error: " << What(result.error()) << "\n";
    return kRuntimeError;
  }

  PrintRelation(result.value(), parsed.required_order.has_value());

  if (args.stats) {
    std::cerr << "STATS plan_cost=" << plan_cost
              << " naive_plan_cost=" << naive_plan_cost
              << " exec_us=" << exec_us
              << " rows=" << result.value().tuples.size() << "\n";
  }
  return kOk;
}
