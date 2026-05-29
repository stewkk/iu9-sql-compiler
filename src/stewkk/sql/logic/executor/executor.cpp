#include <stewkk/sql/logic/executor/executor.hpp>

#include <algorithm>
#include <ranges>
#include <cmath>
#include <unordered_map>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/scope/scope_fail.hpp>

#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/utils/log.hpp>

namespace stewkk::sql {

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

Type GetExpressionType(const Expression& expr, const AttributesInfo& available_attrs) {
  struct ExpressionTypeVisitor {
    Type operator()(const BinaryExpression& binop) const {
      auto lhs_type = std::visit(*this, *binop.lhs);
      auto rhs_type = std::visit(*this, *binop.rhs);
      if (lhs_type != rhs_type) {
        throw std::logic_error{"types mismatch"};
      }
      if (std::ranges::contains(std::vector{BinaryOp::kPlus, BinaryOp::kMinus, BinaryOp::kMul,
                                            BinaryOp::kDiv, BinaryOp::kMod, BinaryOp::kPow},
                                binop.binop)) {
        if (lhs_type != Type::kInt) {
          throw std::logic_error{"types mismatch"};
        }
        return Type::kInt;
      }
      if (std::ranges::contains(std::vector{BinaryOp::kAnd, BinaryOp::kOr}, binop.binop)) {
        if (lhs_type != Type::kBool) {
          throw std::logic_error{"types mismatch"};
        }
        return Type::kBool;
      }
      if (lhs_type == Type::kString
          && !std::ranges::contains(std::vector{BinaryOp::kEq, BinaryOp::kNotEq}, binop.binop)) {
        throw std::logic_error{"strings support only = and != operators"};
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
      if (unop.op == UnaryOp::kIsNull) {
        return Type::kBool;
      }
      if (child_type != Type::kBool) {
        throw std::logic_error{"types mismatch"};
      }
      return Type::kBool;
    }
    Type operator()(const InExpression& in) const {
      auto lhs_type = std::visit(*this, *in.lhs);
      for (const auto& value : in.values) {
        if (const auto* lit = std::get_if<Literal>(&value);
            lit && (*lit == Literal::kNull || *lit == Literal::kUnknown)) {
          continue;
        }
        auto value_type = std::visit(*this, value);
        if (value_type != lhs_type) {
          throw std::logic_error{"types mismatch"};
        }
      }
      return Type::kBool;
    }
    Type operator()(const IntConst& iconst) const {
      return Type::kInt;
    }
    Type operator()(const StringConst& sconst) const {
      return Type::kString;
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
    Type operator()(const InExpression& in) const {
      return Type::kBool;
    }
    Type operator()(const IntConst& iconst) const {
      return Type::kInt;
    }
    Type operator()(const StringConst& sconst) const {
      return Type::kString;
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

AttributesInfo GetAttributesAfterProjection(const AttributesInfo& attrs, const PhysicalProjection& proj) {
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

Value CalcExpression(const Tuple& source, const AttributesInfo& source_attrs, const Expression& expr) {
  struct ExpressionVisitor {
    bool ValuesEqual(Value lhs, Value rhs, Type type) {
      switch (type) {
        case Type::kInt:
          return lhs.value.int_value == rhs.value.int_value;
        case Type::kBool:
          return lhs.value.bool_value == rhs.value.bool_value;
        case Type::kString:
          return lhs.value.string_id == rhs.value.string_id;
      }
    }

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
        case BinaryOp::kOr: {
          bool lhs_true = !lhs.is_null && lhs.value.bool_value;
          bool rhs_true = !rhs.is_null && rhs.value.bool_value;
          if (lhs_true || rhs_true) return Value{false, true};
          if (lhs.is_null || rhs.is_null) return Value{true};
          return Value{false, false};
        }
        case BinaryOp::kAnd: {
          bool lhs_false = !lhs.is_null && !lhs.value.bool_value;
          bool rhs_false = !rhs.is_null && !rhs.value.bool_value;
          if (lhs_false || rhs_false) return Value{false, false};
          if (lhs.is_null || rhs.is_null) return Value{true};
          return Value{false, true};
        }
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
        case UnaryOp::kIsNull:
          return Value{false, child.is_null};
      }
    }
    Value operator()(const InExpression& expr) {
      auto lhs = std::visit(*this, *expr.lhs);
      if (lhs.is_null) {
        return Value{true};
      }

      auto lhs_type = GetExpressionTypeUnchecked(*expr.lhs, source_attrs);
      bool saw_null = false;
      for (const auto& value_expr : expr.values) {
        auto value = std::visit(*this, value_expr);
        if (value.is_null) {
          saw_null = true;
          continue;
        }
        if (ValuesEqual(lhs, value, lhs_type)) {
          return Value{false, !expr.negated};
        }
      }

      if (saw_null) {
        return Value{true};
      }
      return Value{false, expr.negated};
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
    Value operator()(const StringConst& expr) {
      return Value{false, {.string_id = InternString(expr)}};
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

Tuple ApplyProjection(const Tuple& source, const AttributesInfo& source_attrs, const std::vector<ExecExpression>& expressions) {
  return expressions | std::views::transform([&](const auto& expression) {
    return expression(source, source_attrs);
  }) | std::ranges::to<Tuple>();
}

bool ApplyFilter(const Tuple& source, const AttributesInfo& source_attrs, const ExecExpression& filter) {
  auto v = filter(source, source_attrs);
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

bool ContainsStringExpression(const Expression& expr, const AttributesInfo& attrs) {
  struct Visitor {
    bool operator()(const BinaryExpression& expr) const {
      return std::visit(*this, *expr.lhs) || std::visit(*this, *expr.rhs);
    }
    bool operator()(const UnaryExpression& expr) const {
      return std::visit(*this, *expr.child);
    }
    bool operator()(const InExpression& expr) const {
      return std::visit(*this, *expr.lhs)
             || std::ranges::any_of(expr.values, [&](const Expression& value) {
                  return std::visit(*this, value);
                });
    }
    bool operator()(const Attribute& attr) const {
      auto it = std::find_if(attrs.begin(), attrs.end(), [&](const AttributeInfo& attr_info) {
        return attr_info.name == attr.name && attr_info.table == attr.table;
      });
      return it != attrs.end() && it->type == Type::kString;
    }
    bool operator()(const StringConst&) const { return true; }
    bool operator()(const IntConst&) const { return false; }
    bool operator()(const Literal&) const { return false; }

    const AttributesInfo& attrs;
  };
  return std::visit(Visitor{attrs}, expr);
}

bool ContainsInExpression(const Expression& expr) {
  struct Visitor {
    bool operator()(const BinaryExpression& expr) const {
      return std::visit(*this, *expr.lhs) || std::visit(*this, *expr.rhs);
    }
    bool operator()(const UnaryExpression& expr) const {
      return std::visit(*this, *expr.child);
    }
    bool operator()(const InExpression&) const { return true; }
    bool operator()(const Attribute&) const { return false; }
    bool operator()(const StringConst&) const { return false; }
    bool operator()(const IntConst&) const { return false; }
    bool operator()(const Literal&) const { return false; }
  };
  return std::visit(Visitor{}, expr);
}

boost::asio::awaitable<ExecExpression> InterpretedExpressionExecutor::GetExpressionExecutor(const Expression& expr, const AttributesInfo& attrs) {
  struct Executor {
    Value operator()(const Tuple& source, const AttributesInfo& source_attrs) {
      return CalcExpression(source, source_attrs, expr);
    }

    const Expression& expr;
  };
  co_return Executor{expr};
}

boost::asio::awaitable<ExecExpression> JitCompiledExpressionExecutor::GetExpressionExecutor(const Expression& expr, const AttributesInfo& attrs) {
  if (ContainsInExpression(expr)) {
    throw std::logic_error{"IN expressions are not supported by JIT"};
  }
  if (ContainsStringExpression(expr, attrs)) {
    throw std::logic_error{"string expressions are not supported by JIT"};
  }
  struct Executor {
    Value operator()(const Tuple& source, const AttributesInfo& source_attrs) {
      Value result;
      compiled_expr(&result, source.data(), source_attrs.data());
      return result;
    }

    JITCompiler::CompiledExpression compiled_expr;
    llvm::orc::ResourceTrackerSP guard;
  };
  auto [compiled_expr, guard] = co_await compiler_.CompileExpression(expr, attrs);
  co_return Executor{std::move(compiled_expr), std::move(guard)};
}

JitCompiledExpressionExecutor::JitCompiledExpressionExecutor(boost::asio::any_io_executor executor) : compiler_(executor) {}

InterpretedExpressionExecutor::InterpretedExpressionExecutor(boost::asio::any_io_executor executor) {}

boost::asio::awaitable<ExecExpression> CachedJitCompiledExpressionExecutor::GetExpressionExecutor(const Expression& expr, const AttributesInfo& attrs) {
  if (ContainsInExpression(expr)) {
    throw std::logic_error{"IN expressions are not supported by JIT"};
  }
  if (ContainsStringExpression(expr, attrs)) {
    throw std::logic_error{"string expressions are not supported by JIT"};
  }
  auto expr_str = ToString(expr);
  if (auto it = cache_.find(expr_str); it != cache_.end()) {
    co_return it->second;
  }

  struct Executor {
    Value operator()(const Tuple& source, const AttributesInfo& source_attrs) {
      Value result;
      compiled_expr(&result, source.data(), source_attrs.data());
      return result;
    }

    JITCompiler::CompiledExpression compiled_expr;
    llvm::orc::ResourceTrackerSP guard;
  };
  auto [compiled_expr, guard] = co_await compiler_.CompileExpression(expr, attrs);
  auto executor = Executor{std::move(compiled_expr), std::move(guard)};
  cache_[expr_str] = executor;
  co_return executor;
}

CachedJitCompiledExpressionExecutor::CachedJitCompiledExpressionExecutor(boost::asio::any_io_executor executor) : compiler_(executor) {}

template <typename ExpressionExecutor>
Executor<ExpressionExecutor>::Executor(SequentialScan seq_scan, boost::asio::any_io_executor executor)
    : sequential_scan_(std::move(seq_scan)), expression_executor_(executor) {}

template <typename ExpressionExecutor>
boost::asio::awaitable<Result<Relation>> Executor<ExpressionExecutor>::Execute(const PhysicalPlanNode& op) {
  auto exec = co_await boost::asio::this_coro::executor;
  auto [attr_chan, tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, op, attr_chan, tuples_chan);

  // If the child throws, its scope_fail closes both channels, which wakes
  // our async_receive with channel_closed. We then await the task to surface
  // the child's real exception instead of the channel_closed wrapper.
  std::exception_ptr eptr;
  AttributesInfo attrs;
  Tuples result;
  try {
    attrs = co_await attr_chan.async_receive(boost::asio::use_awaitable);
    Log("Received attrs in root");
    for (;;) {
      auto buf = co_await ReceiveTuples(tuples_chan);
      if (buf.empty()) {
        break;
      }
      Log("Received {} tuples in root", buf.size());
      std::move(buf.begin(), buf.end(), std::back_inserter(result));
    }
  } catch (...) {
    eptr = std::current_exception();
  }
  try {
    co_await task(boost::asio::use_awaitable);
  } catch (...) {
    eptr = std::current_exception();
  }
  if (eptr) {
    std::rethrow_exception(eptr);
  }
  Log("Total {} tuples in root", result.size());
  co_return Ok(Relation{std::move(attrs), std::move(result)});
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::Execute(const PhysicalPlanNode& op, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  struct ExecuteVisitor{
    boost::asio::awaitable<void> operator()(const SeqScan& seq_scan) {
      co_await executor.sequential_scan_(seq_scan.table, std::string{OutputTable(seq_scan)},
                                         attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const PhysicalProjection& projection) {
      // NOTE: We are using multiset relational algebra projection (i.e. not
      // eleminating duplicate tuples)
      co_await executor.ExecuteProjection(projection, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const PhysicalFilter& filter) {
      co_await executor.ExecuteFilter(filter, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const NestedLoopJoin& join) {
      co_await executor.ExecuteJoin(join, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const NestedLoopCrossJoin& cross_join) {
      co_await executor.ExecuteCrossJoin(cross_join, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const HashJoin& join) {
      co_await executor.ExecuteHashJoin(join, attr_chan, tuples_chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const MergeJoin&) {
      throw std::runtime_error("MergeJoin execution not implemented");
      co_return;
    }
    boost::asio::awaitable<void> operator()(const IndexSeek&) {
      throw std::runtime_error("IndexSeek execution not implemented");
      co_return;
    }
    // FIXME: that's in-memory sort
    boost::asio::awaitable<void> operator()(const PhysicalSort& sort) {
      auto close_on_fail = boost::scope::make_scope_fail(
          [this] { attr_chan.close(); tuples_chan.close(); });
      auto exec = co_await boost::asio::this_coro::executor;
      auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
      auto task = executor.SpawnExecutor(exec, *sort.source, in_attrs_chan, in_tuples_chan);

      auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);
      co_await attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
      attr_chan.close();

      Tuples all_tuples;
      for (;;) {
        auto buf = co_await ReceiveTuples(in_tuples_chan);
        if (buf.empty()) break;
        std::move(buf.begin(), buf.end(), std::back_inserter(all_tuples));
      }

      std::vector<std::pair<size_t, Direction>> key_indices;
      for (const auto& key : sort.keys.keys) {
        auto it = std::find_if(attrs.begin(), attrs.end(),
            [&](const AttributeInfo& a) {
              return a.table == key.table && a.name == key.column;
            });
        if (it == attrs.end())
          throw std::runtime_error{"sort key column not found: " + key.table + "." + key.column};
        key_indices.push_back({static_cast<size_t>(it - attrs.begin()), key.dir});
      }

      std::sort(all_tuples.begin(), all_tuples.end(),
          [&](const Tuple& a, const Tuple& b) {
            for (const auto& [idx, dir] : key_indices) {
              const auto& va = a[idx];
              const auto& vb = b[idx];
              if (va.is_null && vb.is_null) continue;
              if (va.is_null) return false;
              if (vb.is_null) return true;
              if (va.value.int_value != vb.value.int_value)
                return dir == Direction::kAsc
                    ? va.value.int_value < vb.value.int_value
                    : va.value.int_value > vb.value.int_value;
            }
            return false;
          });

      if (!all_tuples.empty())
        co_await tuples_chan.async_send(boost::system::error_code{},
            std::move(all_tuples), boost::asio::use_awaitable);
      co_await task(boost::asio::use_awaitable);
      tuples_chan.close();
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
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteProjection(const PhysicalProjection& proj,
                                                         AttributesInfoChannel& out_attr_chan,
                                                         TuplesChannel& out_tuples_chan) {
  Log("Executing projection");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { out_attr_chan.close(); out_tuples_chan.close(); });
  auto exec = co_await boost::asio::this_coro::executor;
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, *proj.source, in_attrs_chan, in_tuples_chan);

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
    Log("Received {} tuples in projection", buf.size());
    buf = buf | std::views::transform([&](const auto& tuple) {
      return ApplyProjection(tuple, attrs, executors);
    }) | std::ranges::to<Tuples>();
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                        boost::asio::use_awaitable);
  }
  co_await task(boost::asio::use_awaitable);
  out_tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteFilter(const PhysicalFilter& filter,
                                                     AttributesInfoChannel& out_attr_chan,
                                                     TuplesChannel& out_tuples_chan) {
  Log("Executing filter");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { out_attr_chan.close(); out_tuples_chan.close(); });
  auto exec = co_await boost::asio::this_coro::executor;
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, *filter.source, in_attrs_chan, in_tuples_chan);

  auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);
  Log("Filter received attrs");

  if (GetExpressionType(filter.predicate, attrs) != Type::kBool) {
    throw std::logic_error{"filter expr should return bool"};
  }

  Log("Filter sending attrs");
  co_await out_attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  out_attr_chan.close();
  Log("Filter sent attrs");

  auto filter_executor = co_await expression_executor_.GetExpressionExecutor(filter.predicate, attrs);

  Tuples output_buf;
  output_buf.reserve(kBufSize);
  for (;;) {
    auto input_buf = co_await ReceiveTuples(in_tuples_chan);
    if (input_buf.empty()) {
      break;
    }
    Log("Received {} tuples in filter", input_buf.size());
    auto filtered_view = input_buf | std::views::filter([&](const auto& tuple) {
                           return ApplyFilter(tuple, attrs, filter_executor);
                         }) | std::views::as_rvalue;
    for (auto&& tuple : filtered_view) {
      output_buf.push_back(std::move(tuple));
      if (output_buf.size() == kBufSize) {
        Log("Sending {} tuples in filter", output_buf.size());
        co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(output_buf),
                                            boost::asio::use_awaitable);
        output_buf.clear();
      }
    }
  }
  Log("{} tuples left in output_buf", output_buf.size());
  if (!output_buf.empty()) {
    Log("Sending {} tuples in filter", output_buf.size());
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(output_buf),
                                        boost::asio::use_awaitable);
  }
  co_await task(boost::asio::use_awaitable);
  out_tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteCrossJoin(const NestedLoopCrossJoin& cross_join,
                                                        AttributesInfoChannel& attr_chan,
                                                        TuplesChannel& tuples_chan) {
  Log("Executing cross join");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { attr_chan.close(); tuples_chan.close(); });
  auto exec = co_await boost::asio::this_coro::executor;
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  auto lhs_task = SpawnExecutor(exec, *cross_join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  auto rhs_task = SpawnExecutor(exec, *cross_join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto attrs = co_await ConcatAttrs(lhs_attrs_chan, rhs_attrs_chan);
  Log("Cross join received attrs");
  Log("Cross join sending attrs");
  co_await attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  attr_chan.close();
  Log("Cross join sent attrs");

  auto reader = co_await MaterializeChannel(lhs_tuples_chan);
  Log("Materialized tuples in cross join");

  for (;;) {
    auto buf_rhs = co_await ReceiveTuples(rhs_tuples_chan);
    if (buf_rhs.empty()) {
      break;
    }
    Log("Received {} tuples in cross join as rhs", buf_rhs.size());

    reader.Rewind();
    for (;;) {
      auto buf_lhs = reader.Read();
      if (buf_lhs.empty()) {
        break;
      }
      Log("Read {} tuples back from materialized form", buf_lhs.size());
      for (const auto& tuple_lhs : buf_lhs) {
        Tuples buf_joined;
        buf_joined.reserve(kBufSize);
        // NOTE: not optimal if rhs is a small table (<< kBufSize tuples)
        for (const auto& tuple_rhs : buf_rhs) {
          auto joined_tuple = ConcatTuples(tuple_lhs, tuple_rhs);
          buf_joined.push_back(std::move(joined_tuple));
        }
        Log("Sending {} tuples from cross join", buf_joined.size());
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_joined),
                                        boost::asio::use_awaitable);
      }
    }
  }
  co_await lhs_task(boost::asio::use_awaitable);
  co_await rhs_task(boost::asio::use_awaitable);
  tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteJoin(const NestedLoopJoin& join,
                                                   AttributesInfoChannel& attr_chan,
                                                   TuplesChannel& tuples_chan) {
  Log("Executing join");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { attr_chan.close(); tuples_chan.close(); });
  if (join.type == JoinType::kLeft) {
    std::swap(*join.lhs, *join.rhs);
  }
  auto exec = co_await boost::asio::this_coro::executor;
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  auto lhs_task = SpawnExecutor(exec, *join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  auto rhs_task = SpawnExecutor(exec, *join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto attrs = co_await ConcatAttrs(lhs_attrs_chan, rhs_attrs_chan);
  Log("Join received attrs");
  Log("Join sending attrs");
  co_await attr_chan.async_send(boost::system::error_code{}, attrs, boost::asio::use_awaitable);
  attr_chan.close();
  Log("Join sent attrs");

  auto qual_executor = co_await expression_executor_.GetExpressionExecutor(join.qual, attrs);

  if (join.type == JoinType::kFull) {
    std::vector<Tuple> lhs_all;
    {
      auto full_reader = co_await MaterializeChannel(lhs_tuples_chan);
      full_reader.Rewind();
      for (;;) {
        auto buf = full_reader.Read();
        if (buf.empty()) break;
        std::move(buf.begin(), buf.end(), std::back_inserter(lhs_all));
      }
    }
    std::vector<bool> lhs_matched(lhs_all.size(), false);

    for (;;) {
      auto buf_rhs = co_await ReceiveTuples(rhs_tuples_chan);
      if (buf_rhs.empty()) break;
      Log("Received {} tuples in full join as rhs", buf_rhs.size());

      for (const auto& tuple_rhs : buf_rhs) {
        bool rhs_matched = false;
        Tuples buf_res;
        buf_res.reserve(kBufSize);
        for (size_t i = 0; i < lhs_all.size(); ++i) {
          auto joined = ConcatTuples(lhs_all[i], tuple_rhs);
          auto qual_result = qual_executor(joined, attrs);
          if (!qual_result.is_null && qual_result.value.bool_value) {
            buf_res.push_back(std::move(joined));
            lhs_matched[i] = true;
            rhs_matched = true;
            if (buf_res.size() == kBufSize) {
              co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_res),
                                              boost::asio::use_awaitable);
              buf_res.clear();
              buf_res.reserve(kBufSize);
            }
          }
        }
        if (!rhs_matched) {
          auto rhs_size = tuple_rhs.size();
          auto lhs_size = attrs.size() - rhs_size;
          buf_res.push_back(ConcatTuples(Tuple(lhs_size, Value{true}), tuple_rhs));
        }
        if (!buf_res.empty()) {
          co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_res),
                                          boost::asio::use_awaitable);
        }
      }
    }

    Tuples buf_unmatched;
    buf_unmatched.reserve(kBufSize);
    for (size_t i = 0; i < lhs_all.size(); ++i) {
      if (!lhs_matched[i]) {
        auto lhs_size = lhs_all[i].size();
        auto rhs_size = attrs.size() - lhs_size;
        buf_unmatched.push_back(ConcatTuples(lhs_all[i], Tuple(rhs_size, Value{true})));
        if (buf_unmatched.size() == kBufSize) {
          co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_unmatched),
                                          boost::asio::use_awaitable);
          buf_unmatched.clear();
          buf_unmatched.reserve(kBufSize);
        }
      }
    }
    if (!buf_unmatched.empty()) {
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_unmatched),
                                      boost::asio::use_awaitable);
    }

    co_await lhs_task(boost::asio::use_awaitable);
    co_await rhs_task(boost::asio::use_awaitable);
    tuples_chan.close();
    co_return;
  }

  auto reader = co_await MaterializeChannel(lhs_tuples_chan);
  for (;;) {
    auto buf_rhs = co_await ReceiveTuples(rhs_tuples_chan);
    if (buf_rhs.empty()) {
      break;
    }
    Log("Received {} tuples in join as rhs", buf_rhs.size());

    std::vector<char> used(buf_rhs.size(), false);
    reader.Rewind();
    for (;;) {
      auto buf_lhs = reader.Read();
      if (buf_lhs.empty()) {
        break;
      }
      Log("Read {} tuples back from materialized form", buf_lhs.size());

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
          Log("Sending {} tuples from join", buf_res.size());
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
        Log("Sending {} tuples from join", buf_res.size());
        co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf_res),
                                        boost::asio::use_awaitable);
      }
    }
  }
  co_await lhs_task(boost::asio::use_awaitable);
  co_await rhs_task(boost::asio::use_awaitable);
  tuples_chan.close();
}

namespace {

size_t FindAttrIndex(const AttributesInfo& attrs, const Attribute& a) {
  auto it = std::find_if(attrs.begin(), attrs.end(), [&](const AttributeInfo& ai) {
    return ai.table == a.table && ai.name == a.name;
  });
  if (it == attrs.end()) {
    throw std::runtime_error{"hash join key attribute not found: " + a.table + "." + a.name};
  }
  return static_cast<size_t>(it - attrs.begin());
}

} // namespace

// Inner equi-join: build a hash table on lhs.key, then probe with rhs.
// ImplementHashJoin gates applicability to `attr = attr` quals on Inner joins,
// so the only shapes that reach here are: lhs_attr == rhs_attr or rhs_attr ==
// lhs_attr. We resolve which side owns each attribute via the lhs attribute
// stream; the remaining one must belong to rhs.
template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteHashJoin(
    const HashJoin& join, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  Log("Executing hash join");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { attr_chan.close(); tuples_chan.close(); });
  if (join.type != JoinType::kInner) {
    throw std::logic_error{"HashJoin executor supports only Inner joins"};
  }
  const auto* bin = std::get_if<BinaryExpression>(&join.qual);
  if (!bin || bin->binop != BinaryOp::kEq) {
    throw std::logic_error{"HashJoin qual must be an equality"};
  }
  const auto* a = std::get_if<Attribute>(bin->lhs.get());
  const auto* b = std::get_if<Attribute>(bin->rhs.get());
  if (!a || !b) {
    throw std::logic_error{"HashJoin qual must be `attr = attr`"};
  }

  auto exec = co_await boost::asio::this_coro::executor;
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  auto lhs_task = SpawnExecutor(exec, *join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  auto rhs_task = SpawnExecutor(exec, *join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto lhs_attrs = co_await lhs_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto rhs_attrs = co_await rhs_attrs_chan.async_receive(boost::asio::use_awaitable);

  // Pick the (lhs_key, rhs_key) pair regardless of which side the qual wrote first.
  auto lhs_has = [&](const Attribute& attr) {
    return std::any_of(lhs_attrs.begin(), lhs_attrs.end(), [&](const AttributeInfo& ai) {
      return ai.table == attr.table && ai.name == attr.name;
    });
  };
  const Attribute* lhs_attr = nullptr;
  const Attribute* rhs_attr = nullptr;
  if (lhs_has(*a)) { lhs_attr = a; rhs_attr = b; }
  else if (lhs_has(*b)) { lhs_attr = b; rhs_attr = a; }
  else {
    throw std::runtime_error{"HashJoin qual: neither side of equality found in lhs attrs"};
  }

  size_t lhs_key_idx = FindAttrIndex(lhs_attrs, *lhs_attr);
  size_t rhs_key_idx = FindAttrIndex(rhs_attrs, *rhs_attr);

  AttributesInfo out_attrs = lhs_attrs;
  std::ranges::copy(rhs_attrs, std::back_inserter(out_attrs));
  co_await attr_chan.async_send(boost::system::error_code{}, out_attrs,
                                boost::asio::use_awaitable);
  attr_chan.close();

  // Build phase: collect all lhs tuples into a hash multimap keyed by lhs.key.
  // NULL keys never match (SQL = on NULL is unknown), so drop them.
  std::unordered_multimap<int64_t, Tuple> build;
  for (;;) {
    auto buf = co_await ReceiveTuples(lhs_tuples_chan);
    if (buf.empty()) break;
    Log("HashJoin build received {} tuples", buf.size());
    for (auto& t : buf) {
      const Value& k = t[lhs_key_idx];
      if (k.is_null) continue;
      build.emplace(k.value.int_value, std::move(t));
    }
  }
  Log("HashJoin build phase done; {} entries", build.size());

  // Probe phase: stream rhs, lookup matches, emit joined tuples in kBufSize chunks.
  Tuples out_buf;
  out_buf.reserve(kBufSize);
  for (;;) {
    auto buf = co_await ReceiveTuples(rhs_tuples_chan);
    if (buf.empty()) break;
    Log("HashJoin probe received {} tuples", buf.size());
    for (const auto& rt : buf) {
      const Value& k = rt[rhs_key_idx];
      if (k.is_null) continue;
      auto range = build.equal_range(k.value.int_value);
      for (auto it = range.first; it != range.second; ++it) {
        out_buf.push_back(ConcatTuples(it->second, rt));
        if (out_buf.size() == kBufSize) {
          co_await tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                          boost::asio::use_awaitable);
          out_buf.clear();
          out_buf.reserve(kBufSize);
        }
      }
    }
  }
  if (!out_buf.empty()) {
    co_await tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                    boost::asio::use_awaitable);
  }
  co_await lhs_task(boost::asio::use_awaitable);
  co_await rhs_task(boost::asio::use_awaitable);
  tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::experimental::promise<void(std::exception_ptr)>
Executor<ExpressionExecutor>::SpawnExecutor(boost::asio::any_io_executor exec,
                                             const PhysicalPlanNode& op,
                                             AttributesInfoChannel& attr_chan,
                                             TuplesChannel& tuple_chan) {
  return boost::asio::co_spawn(exec, Execute(op, attr_chan, tuple_chan),
                               boost::asio::experimental::use_promise);
}

template class Executor<InterpretedExpressionExecutor>;
template class Executor<JitCompiledExpressionExecutor>;
template class Executor<CachedJitCompiledExpressionExecutor>;

}  // namespace stewkk::sql
