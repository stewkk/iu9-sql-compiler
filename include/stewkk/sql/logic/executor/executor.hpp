#pragma once

#include <algorithm>
#include <ranges>
#include <cmath>
#include <iostream>
#include <string>
#include <functional>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/logic/executor/materialization.hpp>
#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/channel.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/executor/llvm.hpp>

namespace stewkk::sql {

using ExecExpression = std::function<Value(const Tuple& source, const AttributesInfo& source_attrs)>;

class InterpretedExpressionExecutor {
  public:
    explicit InterpretedExpressionExecutor(boost::asio::any_io_executor executor);
    boost::asio::awaitable<ExecExpression> GetExpressionExecutor(const Expression& expr, const AttributesInfo& attrs);
};

class JitCompiledExpressionExecutor {
  public:
    explicit JitCompiledExpressionExecutor(boost::asio::any_io_executor executor);
    boost::asio::awaitable<ExecExpression> GetExpressionExecutor(const Expression& expr, const AttributesInfo& attrs);
  private:
    JITCompiler compiler_;
};

template <typename ExpressionExecutor = InterpretedExpressionExecutor>
class Executor {
public:
  using SequentialScan = std::function<boost::asio::awaitable<Result<>>(
      const std::string& table_name, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan)>;
  Executor(SequentialScan seq_scan, boost::asio::any_io_executor executor);

  boost::asio::awaitable<Result<Relation>> Execute(const Operator& op);
private:
  boost::asio::awaitable<void> Execute(const Operator& op, AttributesInfoChannel& attr_chan,
                                       TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteProjection(const Projection& proj, AttributesInfoChannel& attr_chan,
                                                 TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteFilter(const Filter& filter, AttributesInfoChannel& attr_chan,
                                             TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteCrossJoin(const CrossJoin& cross_join,
                                                AttributesInfoChannel& attr_chan,
                                                TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteJoin(const Join& join, AttributesInfoChannel& attr_chan,
                                           TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> SpawnExecutor(const Operator& op, AttributesInfoChannel& attr_chan, TuplesChannel& tuple_chan);

private:
  SequentialScan sequential_scan_;
  ExpressionExecutor expression_executor_;
};

Type GetExpressionType(const Expression& expr, const AttributesInfo& available_attrs);
Type GetExpressionTypeUnchecked(const Expression& expr, const AttributesInfo& available_attrs);
AttributesInfo GetAttributesAfterProjection(const AttributesInfo& attrs, const Projection& proj);

template <typename Op, typename Ret>
  requires std::invocable<Op, int64_t, int64_t>
           && std::same_as<std::invoke_result_t<Op, int64_t, int64_t>, Ret>
Value ApplyIntegersOperator(Value lhs, Value rhs) {
  if (lhs.is_null || rhs.is_null) {
    return Value{true};
  }
  return Value{false, Op{}(lhs.value.int_value, rhs.value.int_value)};
}

template <typename Op>
  requires std::invocable<Op, bool, bool>
           && std::same_as<std::invoke_result_t<Op, bool, bool>, bool>
Value ApplyBooleanOperator(Value lhs, Value rhs) {
  if (lhs.is_null || rhs.is_null) {
    return Value{true};
  }
  return Value{false, Op{}(lhs.value.bool_value, rhs.value.bool_value)};
}

struct IntPow {
  int64_t operator()(int64_t base, int64_t exp) const {
    return static_cast<int64_t>(std::pow(base, exp));
  }
};

Value CalcExpression(const Tuple& source, const AttributesInfo& source_attrs, const Expression& expr);
Tuple ApplyProjection(const Tuple& source, const AttributesInfo& source_attrs, const std::vector<ExecExpression>& expressions);
bool ApplyFilter(const Tuple& source, const AttributesInfo& source_attrs, const ExecExpression& filter);
boost::asio::awaitable<std::pair<AttributesInfoChannel, TuplesChannel>> GetChannels();
boost::asio::awaitable<Tuples> ReceiveTuples(TuplesChannel& chan);
boost::asio::awaitable<AttributesInfo> ConcatAttrs(AttributesInfoChannel& lhs_attrs_chan, AttributesInfoChannel& rhs_attrs_chan);
boost::asio::awaitable<DiskFileReader> MaterializeChannel(TuplesChannel& tuples_chan);
Tuple ConcatTuples(const Tuple& lhs, const Tuple& rhs);

template <typename ExpressionExecutor>
Executor<ExpressionExecutor>::Executor(SequentialScan seq_scan, boost::asio::any_io_executor executor)
    : sequential_scan_(std::move(seq_scan)), expression_executor_(executor) {}

template <typename ExpressionExecutor>
boost::asio::awaitable<Result<Relation>> Executor<ExpressionExecutor>::Execute(const Operator& op) {
  auto [attr_chan, tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(op, attr_chan, tuples_chan);

  auto attrs = co_await attr_chan.async_receive(boost::asio::use_awaitable);
#ifdef DEBUG
  std::clog << "Received attrs in root\n";
#endif

  Tuples result;
  for (;;) {
    auto buf = co_await ReceiveTuples(tuples_chan);
    if (buf.empty()) {
      break;
    }
#ifdef DEBUG
    std::clog << std::format("Received {} tuples in root\n", buf.size());
#endif
    std::move(buf.begin(), buf.end(), std::back_inserter(result));
  }

#ifdef DEBUG
  std::clog << std::format("Total {} tuples in root\n", result.size());
#endif
  co_return Ok(Relation{std::move(attrs), std::move(result)});
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::Execute(const Operator& op, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  struct ExecuteVisitor{
    boost::asio::awaitable<void> operator()(const Table& table) {
      co_await executor.sequential_scan_(table.name, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Projection& projection) {
      // NOTE: We are using multiset relational algebra projection (i.e. not
      // eleminating duplicate tuples)
      co_await executor.ExecuteProjection(projection, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Filter& filter) {
      co_await executor.ExecuteFilter(filter, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Join& join) {
      co_await executor.ExecuteJoin(join, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const CrossJoin& cross_join) {
      co_await executor.ExecuteCrossJoin(cross_join, attr_chan, tuples_chan);
      co_return;
    }

    AttributesInfoChannel& attr_chan;
    TuplesChannel& tuples_chan;
    Executor& executor;
  };
  co_await std::visit(ExecuteVisitor{attr_chan, tuples_chan, *this}, op);
  co_return;
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteProjection(const Projection& proj,
                                                         AttributesInfoChannel& out_attr_chan,
                                                         TuplesChannel& out_tuples_chan) {
#ifdef DEBUG
  std::clog << "Executing projection\n";
#endif
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*proj.source, in_attrs_chan, in_tuples_chan);

  auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto attrs_after = GetAttributesAfterProjection(attrs, proj);
  co_await out_attr_chan.async_send(boost::system::error_code{}, attrs_after,
                                    boost::asio::use_awaitable);
  out_attr_chan.close();

  std::vector<ExecExpression> executors;
  executors.reserve(proj.expressions.size());
  for (const auto& expr : proj.expressions) {
    executors.push_back(co_await expression_executor_.GetExpressionExecutor(expr, attrs));
  }

  for (;;) {
    auto buf = co_await ReceiveTuples(in_tuples_chan);
    if (buf.empty()) {
      break;
    }
#ifdef DEBUG
    std::clog << std::format("Received {} tuples in projection\n", buf.size());
#endif
    buf = buf | std::views::transform([&](const auto& tuple) {
      return ApplyProjection(tuple, attrs, executors);
    }) | std::ranges::to<Tuples>();
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                        boost::asio::use_awaitable);
  }
  out_tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteFilter(const Filter& filter,
                                                     AttributesInfoChannel& out_attr_chan,
                                                     TuplesChannel& out_tuples_chan) {
#ifdef DEBUG
  std::clog << "Executing filter\n";
#endif
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*filter.source, in_attrs_chan, in_tuples_chan);

  auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);
#ifdef DEBUG
  std::clog << "Filter received attrs\n";
#endif

  if (GetExpressionType(filter.expr, attrs) != Type::kBool) {
    throw std::logic_error{"filter expr should return bool"};
  }

#ifdef DEBUG
  std::clog << "Filter sending attrs\n";
#endif
  co_await out_attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  out_attr_chan.close();
#ifdef DEBUG
  std::clog << "Filter sent attrs\n";
#endif

  auto filter_executor = co_await expression_executor_.GetExpressionExecutor(filter.expr, attrs);

  Tuples output_buf;
  output_buf.reserve(kBufSize);
  for (;;) {
    auto input_buf = co_await ReceiveTuples(in_tuples_chan);
    if (input_buf.empty()) {
      break;
    }
#ifdef DEBUG
    std::clog << std::format("Received {} tuples in filter\n", input_buf.size());
#endif
    auto filtered_view = input_buf | std::views::filter([&](const auto& tuple) {
                           return ApplyFilter(tuple, attrs, filter_executor);
                         }) | std::views::as_rvalue;
    for (auto&& tuple : filtered_view) {
      output_buf.push_back(std::move(tuple));
      if (output_buf.size() == kBufSize) {
#ifdef DEBUG
        std::clog << std::format("Sending {} tuples in filter\n", output_buf.size());
#endif
        co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(output_buf),
                                            boost::asio::use_awaitable);
        output_buf.clear();
      }
    }
  }
#ifdef DEBUG
  std::clog << std::format("{} tuples left in output_buf\n", output_buf.size());
#endif
  if (!output_buf.empty()) {
#ifdef DEBUG
    std::clog << std::format("Sending {} tuples in filter\n", output_buf.size());
#endif
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(output_buf),
                                        boost::asio::use_awaitable);
  }
  out_tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteCrossJoin(const CrossJoin& cross_join,
                                                        AttributesInfoChannel& attr_chan,
                                                        TuplesChannel& tuples_chan) {
  std::clog << "Executing cross join\n";
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*cross_join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*cross_join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto attrs = co_await ConcatAttrs(lhs_attrs_chan, rhs_attrs_chan);
  std::clog << "Cross join received attrs\n";

  std::clog << "Cross join sending attrs\n";
  co_await attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  attr_chan.close();
  std::clog << "Cross join sent attrs\n";

  auto reader = co_await MaterializeChannel(lhs_tuples_chan);
  std::clog << std::format("Materialized tuples in cross join\n");

  for (;;) {
    auto buf_rhs = co_await ReceiveTuples(rhs_tuples_chan);
    if (buf_rhs.empty()) {
      break;
    }
    std::clog << std::format("Received {} tuples in cross join as rhs\n", buf_rhs.size());

    for (;;) {
      auto buf_lhs = reader.Read();
      if (buf_lhs.empty()) {
        break;
      }
      std::clog << std::format("Read {} tuples back from materialized form\n", buf_lhs.size());
      for (const auto& tuple_lhs : buf_lhs) {
        Tuples buf_joined;
        buf_joined.reserve(kBufSize);
        // NOTE: not optimal if rhs is a small table (<< kBufSize tuples)
        for (const auto& tuple_rhs : buf_rhs) {
          auto joined_tuple = ConcatTuples(tuple_lhs, tuple_rhs);
          buf_joined.push_back(std::move(joined_tuple));
        }
#ifdef DEBUG
        std::clog << std::format("Sending {} tuples from cross join\n", buf_joined.size());
#endif
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_joined),
                                        boost::asio::use_awaitable);
      }
    }
  }
  tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteJoin(const Join& join,
                                                   AttributesInfoChannel& attr_chan,
                                                   TuplesChannel& tuples_chan) {
#ifdef DEBUG
  std::clog << "Executing join\n";
#endif
  if (join.type == JoinType::kFull) {
    throw std::logic_error{"Full joins are not supported by executor"};
  }
  if (join.type == JoinType::kLeft) {
    std::swap(*join.lhs, *join.rhs);
  }
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto attrs = co_await ConcatAttrs(lhs_attrs_chan, rhs_attrs_chan);
#ifdef DEBUG
  std::clog << "Join received attrs\n";
#endif

#ifdef DEBUG
  std::clog << "Join sending attrs\n";
#endif
  co_await attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  attr_chan.close();
#ifdef DEBUG
  std::clog << "Join sent attrs\n";
#endif

  auto qual_executor = co_await expression_executor_.GetExpressionExecutor(join.qual, attrs);

  auto reader = co_await MaterializeChannel(lhs_tuples_chan);
  for (;;) {
    auto buf_rhs = co_await ReceiveTuples(rhs_tuples_chan);
    if (buf_rhs.empty()) {
      break;
    }
#ifdef DEBUG
    std::clog << std::format("Received {} tuples in join as rhs\n", buf_rhs.size());
#endif

    std::vector<char> used(buf_rhs.size(), false);
    for (;;) {
      auto buf_lhs = reader.Read();
      if (buf_lhs.empty()) {
        break;
      }
#ifdef DEBUG
      std::clog << std::format("Read {} tuples back from materialized form\n", buf_lhs.size());
#endif

      for (const auto& tuple_lhs : buf_lhs) {
        Tuples buf_res;
        buf_res.reserve(kBufSize);
        for (const auto& [rhs_index, tuple_rhs] : buf_rhs | std::views::enumerate) {
          auto joined_tuple = ConcatTuples(tuple_lhs, tuple_rhs);
          auto qual_expr_res = qual_executor(joined_tuple, attrs);
          if (qual_expr_res.value.bool_value) {
            buf_res.push_back(std::move(joined_tuple));
            used[rhs_index] = true;
          }
        }
        if (!buf_res.empty()) {
#ifdef DEBUG
          std::clog << std::format("Sending {} tuples from join\n", buf_res.size());
#endif
          co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_res),
                                          boost::asio::use_awaitable);
        }
      }
    }

    if (join.type == JoinType::kRight || join.type == JoinType::kLeft) {
      Tuples buf_res;
      buf_res.reserve(kBufSize);
      for (auto [rhs_index, is_used] : used | std::views::enumerate) {
        if (is_used) {
          continue;
        }

        auto rhs_tuple = std::move(buf_rhs[rhs_index]);

        auto lhs_size = attrs.size() - rhs_tuple.size();
        Tuple lhs_tuple(lhs_size, Value{true});

        auto joined_tuple = ConcatTuples(lhs_tuple, rhs_tuple);
        buf_res.push_back(std::move(joined_tuple));
      }
      if (!buf_res.empty()) {
#ifdef DEBUG
        std::clog << std::format("Sending {} tuples from join\n", buf_res.size());
#endif
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_res),
                                        boost::asio::use_awaitable);
      }
    }
  }
  tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::SpawnExecutor(const Operator& op,
                                                     AttributesInfoChannel& attr_chan,
                                                     TuplesChannel& tuple_chan) {
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::co_spawn(executor, Execute(op, attr_chan, tuple_chan), boost::asio::detached);
}


}  // namespace stewkk::sql
