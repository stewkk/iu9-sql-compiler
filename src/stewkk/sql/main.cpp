#include <algorithm>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ranges>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
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
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>
#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/models/parser/expression.hpp>

namespace stewkk::sql {

namespace {

constexpr int kOk = 0;
constexpr int kUsage = 64;
constexpr int kParseError = 1;
constexpr int kOptimizerError = 2;
constexpr int kRuntimeError = 3;

struct Args {
  std::string data_dir;
  bool print_plan = false;
  bool print_ast = false;
  bool jit = false;
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
    } else {
      std::cerr << "unknown argument: " << a << "\n";
      std::exit(kUsage);
    }
  }
  if (args.data_dir.empty()) {
    std::cerr << "usage: sql --data-dir <csv_dir> [--print-plan] [--jit] < query.sql\n";
    std::exit(kUsage);
  }
  return args;
}

// Build a SchemaCatalog by reading the header of every <table>.csv in the
// directory. Mirrors the conventions used by the query generator and
// CsvDirSequentialScanner: header is "col:type,col:type,...", and files whose
// stem ends in _<digits> are benchmark-only siblings of a base table and share
// its schema, so they are skipped.
SchemaCatalog LoadSchema(const std::string& dir) {
  std::unordered_map<std::string, Schema> tables;
  static const std::regex kBench{R"(_\d+$)"};
  for (const auto& entry : std::filesystem::directory_iterator{dir}) {
    if (entry.path().extension() != ".csv") continue;
    auto stem = entry.path().stem().string();
    if (std::regex_search(stem, kBench)) continue;

    std::ifstream in{entry.path()};
    std::string header;
    if (!std::getline(in, header)) continue;

    Schema schema;
    for (const auto& part : header | std::views::split(',')) {
      std::string token{part.begin(), part.end()};
      auto colon = token.find(':');
      if (colon == std::string::npos) continue;
      schema.push_back(Attribute{stem, token.substr(0, colon)});
    }
    tables.emplace(std::move(stem), std::move(schema));
  }
  return SchemaCatalog{std::move(tables)};
}

std::string SerializeAst(const Operator& op) {
  return std::visit([](auto&& node) -> std::string {
    using T = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<T, Table>) {
      return "(Table " + node.name + ")";
    } else if constexpr (std::is_same_v<T, Filter>) {
      return "(Filter " + ToString(node.expr) + " " + SerializeAst(*node.source) + ")";
    } else if constexpr (std::is_same_v<T, Projection>) {
      std::string exprs;
      for (const auto& e : node.expressions) exprs += " " + ToString(e);
      return "(Projection" + exprs + " " + SerializeAst(*node.source) + ")";
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
  }
  return "?";
}

std::string TypeName(Type t) {
  switch (t) {
    case Type::kInt:
      return "int";
    case Type::kBool:
      return "bool";
  }
  return "?";
}

// Canonical text format the fuzzer compares against MS SQL Server output.
// First line: tab-separated "table.col:type" header (or just ":type" when the
// attribute has no name, e.g. an expression in the projection).
// Following lines: one row each, tab-separated values, "NULL" for nulls.
// Rows are sorted lexicographically unless the query had ORDER BY.
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

// Coroutine declared as a free function (not a captureless lambda) so the
// awaitable holds its parameters by reference into main's stack, sidestepping
// the lifetime trap of an IIFE'd `[&]` lambda whose closure dies before the
// coroutine frame.
boost::asio::awaitable<Result<Relation>> RunQuery(const std::string& data_dir,
                                                  const PhysicalPlanNode& plan) {
  CsvDirSequentialScanner seq_scan{data_dir};
  Executor executor(std::move(seq_scan), co_await boost::asio::this_coro::executor);
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
  try {
    PropertySet required = parsed.required_order
        ? PropertySet{SortProperty{*parsed.required_order}}
        : PropertySet::Any();
    Optimizer optimizer(parsed.op, MakeMainRules(), {}, LoadSchema(args.data_dir),
                        std::move(required));
    plan = optimizer.Optimize();
  } catch (const std::exception& e) {
    std::cerr << "optimizer error: " << e.what() << "\n";
    return kOptimizerError;
  }

  if (args.print_plan) {
    std::cerr << Serialize(plan) << "\n";
  }

  Result<Relation> result;
  try {
    boost::asio::io_context ctx;
    boost::asio::any_io_executor exec = ctx.get_executor();
    auto fut = args.jit
        ? boost::asio::co_spawn(exec, RunQueryJit(args.data_dir, plan),
                                boost::asio::use_future)
        : boost::asio::co_spawn(exec, RunQuery(args.data_dir, plan),
                                boost::asio::use_future);
    ctx.run();
    result = fut.get();
  } catch (const std::exception& e) {
    std::cerr << "runtime error: " << e.what() << "\n";
    return kRuntimeError;
  }

  if (!result.has_value()) {
    std::cerr << "runtime error: " << What(result.error()) << "\n";
    return kRuntimeError;
  }

  PrintRelation(result.value(), parsed.required_order.has_value());
  return kOk;
}
