#include <stewkk/sql/logic/executor/executor.hpp>

#include <algorithm>
#include <ranges>
#include <cmath>
#include <iostream>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/logic/executor/materialization.hpp>

namespace stewkk::sql {

namespace {

Type GetExpressionType(const Expression& expr, const AttributesInfo& available_attrs) {
  struct ExpressionTypeVisitor {
    Type operator()(const BinaryExpression& binop) const {
      auto lhs_type = std::visit(*this, *binop.lhs);
      auto rhs_type = std::visit(*this, *binop.rhs);
      if (lhs_type != rhs_type) {
        throw std::logic_error{"types mismatch"};
      }
      if (std::ranges::contains(std::vector{BinaryOp::kPlus, BinaryOp::kMinus, BinaryOp::kDiv,
                                            BinaryOp::kMod, BinaryOp::kPow},
                                binop.binop)) {
        if (lhs_type != Type::kInt) {
          throw std::logic_error{"types mismatch"};
        }
        return Type::kInt;
      }
      return Type::kBool;
    }
    Type operator()(const UnaryExpression& unop) const {
      auto child_type = std::visit(*this, *unop.child);
      if (unop.op == UnaryOp::kMinus) {
        if (child_type != Type::kInt) {
          throw std::logic_error{"types mismatch"};
        }
        return Type::kInt;
      }
      if (child_type != Type::kBool) {
        throw std::logic_error{"types mismatch"};
      }
      return Type::kBool;
    }
    Type operator()(const IntConst& iconst) const {
      return Type::kInt;
    }
    Type operator()(const Literal& literal) const {
      // NOTE: we are using switch because compiler will remind about adding
      // type of new literal here
      switch (literal) {
        case Literal::kNull:
          return Type::kBool;
        case Literal::kTrue:
          return Type::kBool;
        case Literal::kFalse:
          return Type::kBool;
        case Literal::kUnknown:
          return Type::kBool;
      }
    }
    Type operator()(const Attribute& attr) const {
      auto it = std::find_if(available_attrs.begin(), available_attrs.end(),
                             [&attr](const AttributeInfo& attr_info) {
                               return attr_info.name == attr.name && attr_info.table == attr.table;
                             });
      if (it != available_attrs.end()) {
        return it->type;
      }
      throw std::logic_error{"no such attribute"};
    }

    const AttributesInfo& available_attrs;
  };
  return std::visit(ExpressionTypeVisitor{available_attrs}, expr);
}

Type GetExpressionTypeUnchecked(const Expression& expr, const AttributesInfo& available_attrs) {
  struct ExpressionTypeVisitor {
    Type operator()(const BinaryExpression& binop) const {
      if (std::ranges::contains(std::vector{BinaryOp::kPlus, BinaryOp::kMinus, BinaryOp::kDiv,
                                            BinaryOp::kMod, BinaryOp::kPow},
                                binop.binop)) {
        return Type::kInt;
      }
      return Type::kBool;
    }
    Type operator()(const UnaryExpression& unop) const {
      if (unop.op == UnaryOp::kMinus) {
        return Type::kInt;
      }
      return Type::kBool;
    }
    Type operator()(const IntConst& iconst) const {
      return Type::kInt;
    }
    Type operator()(const Literal& literal) const {
      switch (literal) {
        case Literal::kNull:
          return Type::kBool;
        case Literal::kTrue:
          return Type::kBool;
        case Literal::kFalse:
          return Type::kBool;
        case Literal::kUnknown:
          return Type::kBool;
      }
    }
    Type operator()(const Attribute& attr) const {
      auto it = std::find_if(available_attrs.begin(), available_attrs.end(),
                             [&attr](const AttributeInfo& attr_info) {
                               return attr_info.name == attr.name && attr_info.table == attr.table;
                             });
      if (it != available_attrs.end()) {
        return it->type;
      }
      throw std::logic_error{"no such attribute"};
    }

    const AttributesInfo& available_attrs;
  };
  return std::visit(ExpressionTypeVisitor{available_attrs}, expr);
}

AttributesInfo GetAttributesAfterProjection(const AttributesInfo& attrs, const Projection& proj) {
  AttributesInfo result_attributes;
  result_attributes.reserve(proj.expressions.size());
  for (const auto& target : proj.expressions) {
    AttributeInfo projection_result;
    projection_result.type = GetExpressionType(target, attrs);
    if (const Attribute* attr = std::get_if<Attribute>(&target)) {
      projection_result.name = std::move(attr->name);
      projection_result.table = std::move(attr->table);
    }
    result_attributes.push_back(std::move(projection_result));
  }
  return result_attributes;
}

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

Value CalcExpression(const Tuple& source, const AttributesInfo& source_attrs, const Expression& expr) {
  struct ExpressionVisitor {
    Value operator()(const BinaryExpression& expr) {
      auto lhs = std::visit(*this, *expr.lhs);
      auto rhs = std::visit(*this, *expr.rhs);
      switch (expr.binop) {
        case BinaryOp::kGt:
          if (GetExpressionTypeUnchecked(*expr.lhs, source_attrs) == Type::kBool) {
            return ApplyBooleanOperator<std::greater<void>>(std::move(lhs), std::move(rhs));
          }
          return ApplyIntegersOperator<std::greater<void>, bool>(std::move(lhs), std::move(rhs));
        case BinaryOp::kLt:
          if (GetExpressionTypeUnchecked(*expr.lhs, source_attrs) == Type::kBool) {
            return ApplyBooleanOperator<std::less<void>>(std::move(lhs), std::move(rhs));
          }
          return ApplyIntegersOperator<std::less<void>, bool>(std::move(lhs), std::move(rhs));
        case BinaryOp::kLe:
          if (GetExpressionTypeUnchecked(*expr.lhs, source_attrs) == Type::kBool) {
            return ApplyBooleanOperator<std::less_equal<void>>(std::move(lhs), std::move(rhs));
          }
          return ApplyIntegersOperator<std::less_equal<void>, bool>(std::move(lhs), std::move(rhs));
        case BinaryOp::kGe:
          if (GetExpressionTypeUnchecked(*expr.lhs, source_attrs) == Type::kBool) {
            return ApplyBooleanOperator<std::greater_equal<void>>(std::move(lhs), std::move(rhs));
          }
          return ApplyIntegersOperator<std::greater_equal<void>, bool>(std::move(lhs), std::move(rhs));
        case BinaryOp::kNotEq:
          if (GetExpressionTypeUnchecked(*expr.lhs, source_attrs) == Type::kBool) {
            return ApplyBooleanOperator<std::not_equal_to<void>>(std::move(lhs), std::move(rhs));
          }
          return ApplyIntegersOperator<std::not_equal_to<void>, bool>(std::move(lhs), std::move(rhs));
        case BinaryOp::kEq:
          if (GetExpressionTypeUnchecked(*expr.lhs, source_attrs) == Type::kBool) {
            return ApplyBooleanOperator<std::equal_to<void>>(std::move(lhs), std::move(rhs));
          }
          return ApplyIntegersOperator<std::equal_to<void>, bool>(std::move(lhs), std::move(rhs));
        case BinaryOp::kOr:
          return ApplyBooleanOperator<std::logical_or<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kAnd:
          return ApplyBooleanOperator<std::logical_and<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kPlus:
          return ApplyIntegersOperator<std::plus<int64_t>, int64_t>(std::move(lhs), std::move(rhs));
        case BinaryOp::kMinus:
          return ApplyIntegersOperator<std::minus<int64_t>, int64_t>(std::move(lhs), std::move(rhs));
        case BinaryOp::kMul:
          return ApplyIntegersOperator<std::multiplies<int64_t>, int64_t>(std::move(lhs), std::move(rhs));
        case BinaryOp::kDiv:
          return ApplyIntegersOperator<std::divides<int64_t>, int64_t>(std::move(lhs), std::move(rhs));
        case BinaryOp::kMod:
          return ApplyIntegersOperator<std::modulus<int64_t>, int64_t>(std::move(lhs), std::move(rhs));
        case BinaryOp::kPow:
          return ApplyIntegersOperator<IntPow, int64_t>(std::move(lhs), std::move(rhs));
      }
    }
    Value operator()(const UnaryExpression& expr) {
      auto child = std::visit(*this, *expr.child);

      switch (expr.op) {
        case UnaryOp::kNot:
          if (child.is_null) {
            return Value{true};
          }
          if (child.value.bool_value) {
            return Value{false, false};
          }
          return Value{false, true};
        case UnaryOp::kMinus:
          if (child.is_null) {
            return Value{true};
          }
          return Value{false, -child.value.int_value};
      }
    }
    Value operator()(const Attribute& expr) {
      auto it = std::find_if(source_attrs.begin(), source_attrs.end(),
                             [&expr](const AttributeInfo& attr_info) {
                               return attr_info.name == expr.name && attr_info.table == expr.table;
                             });
      // NOTE: already checked in GetExpressionType
      auto index = it - source_attrs.begin();
      return source[index];
    }
    Value operator()(const IntConst& expr) {
      return Value{false, expr};
    }
    Value operator()(const Literal& expr) {
      switch (expr) {
        case Literal::kNull:
          return Value{true};
        case Literal::kTrue: {
          return Value{false, true};
        }
        case Literal::kFalse: {
          return Value{false, false};
        }
        case Literal::kUnknown: {
          return Value{true};
        }
      }
    }

    const AttributesInfo& source_attrs;
    const Tuple& source;
  };

  return std::visit(ExpressionVisitor{source_attrs, source}, expr);
}

Tuple ApplyProjection(const Tuple& source, const AttributesInfo& source_attrs, const Projection& proj) {
  return proj.expressions | std::views::transform([&](const auto& target) {
    return CalcExpression(source, source_attrs, target);
  }) | std::ranges::to<Tuple>();
}

bool ApplyFilter(const Tuple& source, const AttributesInfo& source_attrs, const Filter& filter) {
  auto v = CalcExpression(source, source_attrs, filter.expr);
  if (v.is_null) {
    return false;
  }
  return v.value.bool_value;
}

boost::asio::awaitable<std::pair<AttributesInfoChannel, TuplesChannel>> GetChannels() {
  auto executor = co_await boost::asio::this_coro::executor;
  co_return std::make_pair(AttributesInfoChannel{executor, 1}, TuplesChannel{executor, 1});
}

boost::asio::awaitable<Tuples> ReceiveTuples(TuplesChannel& chan) {
    Tuples buf;
    try {
      buf = co_await chan.async_receive(boost::asio::use_awaitable);
    } catch (const boost::system::system_error& ex) {}
    co_return buf;
}

boost::asio::awaitable<AttributesInfo> ConcatAttrs(AttributesInfoChannel& lhs_attrs_chan, AttributesInfoChannel& rhs_attrs_chan) {
  auto attrs = co_await lhs_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto rhs_attrs = co_await rhs_attrs_chan.async_receive(boost::asio::use_awaitable);
  std::ranges::copy(std::move(rhs_attrs), std::back_inserter(attrs));
  co_return attrs;
}

boost::asio::awaitable<DiskFileReader> MaterializeChannel(TuplesChannel& tuples_chan) {
  DiskFileWriter writer;
  for (;;) {
    auto buf = co_await ReceiveTuples(tuples_chan);
    if (buf.empty()) {
      break;
    }
    writer.Write(buf);
  }

  co_return std::move(writer).GetDiskFileReader();
}

Tuple ConcatTuples(const Tuple& lhs, const Tuple& rhs) {
  Tuple joined_tuple;
  joined_tuple.reserve(lhs.size() + rhs.size());
  std::ranges::copy(lhs, std::back_inserter(joined_tuple));
  std::ranges::copy(rhs, std::back_inserter(joined_tuple));
  return joined_tuple;
}

} // namespace

Executor::Executor(SequentialScan seq_scan)
    : sequential_scan_(std::move(seq_scan)) {}

boost::asio::awaitable<Result<Relation>> Executor::Execute(const Operator& op) const {
  auto [attr_chan, tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(op, attr_chan, tuples_chan);

  auto attrs = co_await attr_chan.async_receive(boost::asio::use_awaitable);
  std::clog << "Received attrs in root\n";

  Tuples result;
  for (;;) {
    auto buf = co_await ReceiveTuples(tuples_chan);
    if (buf.empty()) {
      break;
    }
    std::clog << std::format("Received {} tuples in root\n", buf.size());
    std::move(buf.begin(), buf.end(), std::back_inserter(result));
  }

  std::clog << std::format("Total {} tuples in root\n", result.size());
  co_return Ok(Relation{std::move(attrs), std::move(result)});
}

boost::asio::awaitable<void> Executor::Execute(const Operator& op, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) const {
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
    const Executor& executor;
  };
  co_await std::visit(ExecuteVisitor{attr_chan, tuples_chan, *this}, op);
  co_return;
}

boost::asio::awaitable<void> Executor::ExecuteProjection(const Projection& proj,
                                                         AttributesInfoChannel& out_attr_chan,
                                                         TuplesChannel& out_tuples_chan) const {
  std::clog << "Executing projection\n";
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*proj.source, in_attrs_chan, in_tuples_chan);

  auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto attrs_after = GetAttributesAfterProjection(attrs, proj);
  co_await out_attr_chan.async_send(boost::system::error_code{}, attrs_after,
                                    boost::asio::use_awaitable);
  out_attr_chan.close();

  for (;;) {
    auto buf = co_await ReceiveTuples(in_tuples_chan);
    if (buf.empty()) {
      break;
    }
    // FIXME: add JIT
    std::clog << std::format("Received {} tuples in projection\n", buf.size());
    buf = buf | std::views::transform([&](const auto& tuple) {
      return ApplyProjection(tuple, attrs, proj);
    }) | std::ranges::to<Tuples>();
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                        boost::asio::use_awaitable);
  }
  out_tuples_chan.close();
}

boost::asio::awaitable<void> Executor::ExecuteFilter(const Filter& filter,
                                                     AttributesInfoChannel& out_attr_chan,
                                                     TuplesChannel& out_tuples_chan) const {
  std::clog << "Executing filter\n";
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  co_await SpawnExecutor(*filter.source, in_attrs_chan, in_tuples_chan);

  auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);
  std::clog << "Filter received attrs\n";

  if (GetExpressionType(filter.expr, attrs) != Type::kBool) {
    throw std::logic_error{"filter expr should return bool"};
  }

  std::clog << "Filter sending attrs\n";
  co_await out_attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  out_attr_chan.close();
  std::clog << "Filter sent attrs\n";

  Tuples output_buf;
  output_buf.reserve(kBufSize);
  for (;;) {
    auto input_buf = co_await ReceiveTuples(in_tuples_chan);
    if (input_buf.empty()) {
      break;
    }
    // FIXME: add JIT
    std::clog << std::format("Received {} tuples in filter\n", input_buf.size());
    auto filtered_view = input_buf | std::views::filter([&](const auto& tuple) {
                           return ApplyFilter(tuple, attrs, filter);
                         }) | std::views::as_rvalue;
    for (auto&& tuple : filtered_view) {
      output_buf.push_back(std::move(tuple));
      if (output_buf.size() == kBufSize) {
        std::clog << std::format("Sending {} tuples in filter\n", output_buf.size());
        co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(output_buf),
                                            boost::asio::use_awaitable);
        output_buf.clear();
      }
    }
  }
  std::cout << std::format("{} tuples left in output_buf\n", output_buf.size());
  if (!output_buf.empty()) {
    std::clog << std::format("Sending {} tuples in filter\n", output_buf.size());
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(output_buf),
                                        boost::asio::use_awaitable);
  }
  out_tuples_chan.close();
}

boost::asio::awaitable<void> Executor::ExecuteCrossJoin(const CrossJoin& cross_join,
                                                        AttributesInfoChannel& attr_chan,
                                                        TuplesChannel& tuples_chan) const {
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
        std::clog << std::format("Sending {} tuples from cross join\n", buf_joined.size());
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_joined),
                                        boost::asio::use_awaitable);
      }
    }
  }
  tuples_chan.close();
}

boost::asio::awaitable<void> Executor::ExecuteJoin(const Join& join,
                                                   AttributesInfoChannel& attr_chan,
                                                   TuplesChannel& tuples_chan) const {
  std::clog << "Executing join\n";
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
  std::clog << "Join received attrs\n";

  std::clog << "Join sending attrs\n";
  co_await attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  attr_chan.close();
  std::clog << "Join sent attrs\n";

  auto reader = co_await MaterializeChannel(lhs_tuples_chan);
  for (;;) {
    auto buf_rhs = co_await ReceiveTuples(rhs_tuples_chan);
    if (buf_rhs.empty()) {
      break;
    }
    std::clog << std::format("Received {} tuples in join as rhs\n", buf_rhs.size());

    std::vector<char> used(buf_rhs.size(), false);
    for (;;) {
      auto buf_lhs = reader.Read();
      if (buf_lhs.empty()) {
        break;
      }
      std::clog << std::format("Read {} tuples back from materialized form\n", buf_lhs.size());

      for (const auto& tuple_lhs : buf_lhs) {
        Tuples buf_res;
        buf_res.reserve(kBufSize);
        for (const auto& [rhs_index, tuple_rhs] : buf_rhs | std::views::enumerate) {
          auto joined_tuple = ConcatTuples(tuple_lhs, tuple_rhs);
          auto qual_expr_res = CalcExpression(joined_tuple, attrs, join.qual);
          if (qual_expr_res.value.bool_value) {
            buf_res.push_back(std::move(joined_tuple));
            used[rhs_index] = true;
          }
        }
        if (!buf_res.empty()) {
          std::clog << std::format("Sending {} tuples from join\n", buf_res.size());
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
        std::clog << std::format("Sending {} tuples from join\n", buf_res.size());
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_res),
                                        boost::asio::use_awaitable);
      }
    }
  }
  tuples_chan.close();
}

boost::asio::awaitable<void> Executor::SpawnExecutor(const Operator& op,
                                                     AttributesInfoChannel& attr_chan,
                                                     TuplesChannel& tuple_chan) const {
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::co_spawn(executor, Execute(op, attr_chan, tuple_chan), boost::asio::detached);
}

}  // namespace stewkk::sql
