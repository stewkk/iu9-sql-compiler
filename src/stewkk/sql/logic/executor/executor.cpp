#include <stewkk/sql/logic/executor/executor.hpp>

#include <algorithm>
#include <format>
#include <ranges>
#include <cmath>
#include <optional>
#include <unordered_map>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/scope/scope_fail.hpp>

#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>
#include <stewkk/sql/utils/log.hpp>

namespace stewkk::sql {

namespace {

Value MakeBooleanValue(bool value) {
  return Value{false, static_cast<int64_t>(value)};
}

}  // namespace

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
  return MakeBooleanValue(Op{}(lhs.value.bool_value, rhs.value.bool_value));
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
          && !std::ranges::contains(std::vector{BinaryOp::kEq, BinaryOp::kNotEq,
                                                BinaryOp::kLt, BinaryOp::kLe,
                                                BinaryOp::kGt, BinaryOp::kGe},
                                    binop.binop)) {
        throw std::logic_error{"strings support only comparison operators"};
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
    Type operator()(const AggregateExpression& aggregate) const {
      if (aggregate.function == AggregateFunction::kCount) {
        return Type::kInt;
      }
      if (aggregate.is_star) {
        throw std::logic_error{"star aggregate has no scalar type"};
      }
      return std::visit(*this, *aggregate.argument);
    }
    Type operator()(const IntConst& iconst) const {
      return Type::kInt;
    }
    Type operator()(const StringConst& sconst) const {
      return Type::kString;
    }
    Type operator()(const Literal& literal) const {
      // NOTE: we are using switch because compiler will remind about adding
     
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
      if (std::ranges::contains(std::vector{BinaryOp::kPlus, BinaryOp::kMinus, BinaryOp::kMul,
                                            BinaryOp::kDiv, BinaryOp::kMod, BinaryOp::kPow},
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
    Type operator()(const AggregateExpression& aggregate) const {
      return Type::kInt;
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
  for (const auto& [index, target] : proj.expressions | std::views::enumerate) {
    AttributeInfo projection_result;
    projection_result.type = GetExpressionType(target, attrs);
    if (index < static_cast<std::ptrdiff_t>(proj.aliases.size()) && proj.aliases[index]) {
      projection_result.name = *proj.aliases[index];
      projection_result.table = "";
    } else if (const Attribute* attr = std::get_if<Attribute>(&target)) {
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

    Value CompareValues(Value lhs, Value rhs, BinaryOp op, Type type) {
      if (lhs.is_null || rhs.is_null) {
        return Value{true};
      }
      int cmp = 0;
      switch (type) {
        case Type::kInt:
          cmp = (lhs.value.int_value > rhs.value.int_value)
              - (lhs.value.int_value < rhs.value.int_value);
          break;
        case Type::kBool:
          cmp = (lhs.value.bool_value > rhs.value.bool_value)
              - (lhs.value.bool_value < rhs.value.bool_value);
          break;
        case Type::kString: {
          const auto& l = GetInternedString(lhs.value.string_id);
          const auto& r = GetInternedString(rhs.value.string_id);
          cmp = (l > r) - (l < r);
          break;
        }
      }
      switch (op) {
        case BinaryOp::kGt:
          return MakeBooleanValue(cmp > 0);
        case BinaryOp::kLt:
          return MakeBooleanValue(cmp < 0);
        case BinaryOp::kLe:
          return MakeBooleanValue(cmp <= 0);
        case BinaryOp::kGe:
          return MakeBooleanValue(cmp >= 0);
        case BinaryOp::kNotEq:
          return MakeBooleanValue(cmp != 0);
        case BinaryOp::kEq:
          return MakeBooleanValue(cmp == 0);
        default:
          std::unreachable();
      }
    }

    Value operator()(const BinaryExpression& expr) {
      auto lhs = std::visit(*this, *expr.lhs);
      auto rhs = std::visit(*this, *expr.rhs);
      switch (expr.binop) {
        case BinaryOp::kGt:
        case BinaryOp::kLt:
        case BinaryOp::kLe:
        case BinaryOp::kGe:
        case BinaryOp::kNotEq:
        case BinaryOp::kEq:
          return CompareValues(std::move(lhs), std::move(rhs), expr.binop,
                               GetExpressionTypeUnchecked(*expr.lhs, source_attrs));
        case BinaryOp::kOr: {
          bool lhs_true = !lhs.is_null && lhs.value.bool_value;
          bool rhs_true = !rhs.is_null && rhs.value.bool_value;
          if (lhs_true || rhs_true) return MakeBooleanValue(true);
          if (lhs.is_null || rhs.is_null) return Value{true};
          return MakeBooleanValue(false);
        }
        case BinaryOp::kAnd: {
          bool lhs_false = !lhs.is_null && !lhs.value.bool_value;
          bool rhs_false = !rhs.is_null && !rhs.value.bool_value;
          if (lhs_false || rhs_false) return MakeBooleanValue(false);
          if (lhs.is_null || rhs.is_null) return Value{true};
          return MakeBooleanValue(true);
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
            return MakeBooleanValue(false);
          }
          return MakeBooleanValue(true);
        case UnaryOp::kMinus:
          if (child.is_null) {
            return Value{true};
          }
          return Value{false, -child.value.int_value};
        case UnaryOp::kIsNull:
          return MakeBooleanValue(child.is_null);
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
          return MakeBooleanValue(!expr.negated);
        }
      }

      if (saw_null) {
        return Value{true};
      }
      return MakeBooleanValue(expr.negated);
    }
    Value operator()(const AggregateExpression&) {
      throw std::logic_error{"aggregate expressions cannot be evaluated as scalar expressions"};
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
          return MakeBooleanValue(true);
        }
        case Literal::kFalse: {
          return MakeBooleanValue(false);
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
    bool operator()(const AggregateExpression& expr) const {
      return !expr.is_star && expr.argument && std::visit(*this, *expr.argument);
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
    bool operator()(const AggregateExpression& expr) const {
      return !expr.is_star && expr.argument && std::visit(*this, *expr.argument);
    }
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
Executor<ExpressionExecutor>::Executor(SequentialScan seq_scan, IndexScan index_scan, boost::asio::any_io_executor executor)
    : sequential_scan_(std::move(seq_scan)), index_scan_(std::move(index_scan)), expression_executor_(executor) {}

template <typename ExpressionExecutor>
boost::asio::awaitable<Result<Relation>> Executor<ExpressionExecutor>::Execute(const PhysicalPlanNode& op) {
  auto exec = co_await boost::asio::this_coro::executor;
  auto [attr_chan, tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, op, attr_chan, tuples_chan);

 
 
 
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
  auto close_on_fail = boost::scope::make_scope_fail([&] {
    attr_chan.close();
    tuples_chan.close();
  });
  struct ExecuteVisitor{
    boost::asio::awaitable<void> operator()(const SeqScan& seq_scan) const {
      return executor.ExecuteSeqScan(seq_scan, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalProjection& projection) const {
      // NOTE: We are using multiset relational algebra projection (i.e. not
      return executor.ExecuteProjection(projection, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalFilter& filter) const {
      return executor.ExecuteFilter(filter, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const NestedLoopJoin& join) const {
      return executor.ExecuteJoin(join, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const NestedLoopCrossJoin& cross_join) const {
      return executor.ExecuteCrossJoin(cross_join, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const HashJoin& join) const {
      return executor.ExecuteHashJoin(join, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalAggregation& agg) const {
      return executor.ExecuteHashAggregate(agg, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalStreamAggregation& agg) const {
      return executor.ExecuteStreamAggregate(agg, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalPartialAggregation& agg) const {
      return executor.ExecuteHashAggregate(
          PhysicalAggregation{agg.source, agg.group_by, agg.aggregates},
          attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalFinalAggregation& agg) const {
      return executor.ExecuteHashAggregate(
          PhysicalAggregation{agg.source, agg.group_by, agg.aggregates},
          attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const MergeJoin& join) const {
      return executor.ExecuteMergeJoin(join, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const IndexSeek& seek) const {
      return executor.ExecuteIndexSeek(seek, attr_chan, tuples_chan);
    }
    boost::asio::awaitable<void> operator()(const PhysicalSort& sort) const {
      return executor.ExecuteSort(sort, attr_chan, tuples_chan);
    }

    AttributesInfoChannel& attr_chan;
    TuplesChannel& tuples_chan;
    Executor& executor;
  };
  co_await std::visit(ExecuteVisitor{attr_chan, tuples_chan, *this}, op.node);
  co_return;
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteSeqScan(
    const SeqScan& seq_scan, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  co_await sequential_scan_(seq_scan.table, std::string{OutputTable(seq_scan)}, attr_chan, tuples_chan);
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteIndexSeek(
    const IndexSeek& seek, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  // FIXME: make index_scan_ required!
  if (!index_scan_) {
    throw std::runtime_error("IndexSeek execution requested, but no index scanner is configured");
  }
  co_await index_scan_(seek.table, seek.alias ? *seek.alias : seek.table, seek.predicate,
                       seek.index_column, attr_chan, tuples_chan);
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteSort(
    const PhysicalSort& sort, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  // FIXME: that's in-memory sort
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { attr_chan.close(); tuples_chan.close(); });
  auto exec = co_await boost::asio::this_coro::executor;
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, *sort.source, in_attrs_chan, in_tuples_chan);

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
          return a.name == key.column && (key.table.empty() || a.table == key.table);
        });
    if (it == attrs.end())
      throw std::runtime_error{"sort key column not found: " + key.table + "." + key.column};
    key_indices.push_back({static_cast<size_t>(it - attrs.begin()), key.dir});
  }

  std::sort(all_tuples.begin(), all_tuples.end(),
      [&](const Tuple& a, const Tuple& b) {
        for (const auto& [idx, dir] : key_indices) {
          const auto& attr = attrs[idx];
          const auto& va = a[idx];
          const auto& vb = b[idx];
          if (va.is_null && vb.is_null) continue;
          if (va.is_null) return false;
          if (vb.is_null) return true;
          bool less = false;
          bool greater = false;
          switch (attr.type) {
            case Type::kInt:
              less = va.value.int_value < vb.value.int_value;
              greater = va.value.int_value > vb.value.int_value;
              break;
            case Type::kBool:
              less = va.value.bool_value < vb.value.bool_value;
              greater = va.value.bool_value > vb.value.bool_value;
              break;
            case Type::kString: {
              const auto& sa = GetInternedString(va.value.string_id);
              const auto& sb = GetInternedString(vb.value.string_id);
              less = sa < sb;
              greater = sa > sb;
              break;
            }
          }
          if (less || greater) return dir == Direction::kAsc ? less : greater;
        }
        return false;
      });

  if (!all_tuples.empty())
    co_await tuples_chan.async_send(boost::system::error_code{},
        std::move(all_tuples), boost::asio::use_awaitable);
  co_await task(boost::asio::use_awaitable);
  tuples_chan.close();
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

bool AttrMatches(const AttributeInfo& ai, const Attribute& a) {
  return ai.name == a.name && (a.table.empty() || ai.table == a.table);
}

bool HasAttr(const AttributesInfo& attrs, const Attribute& a) {
  return std::ranges::any_of(attrs, [&](const AttributeInfo& ai) {
    return AttrMatches(ai, a);
  });
}

size_t FindAttrIndexFlexible(const AttributesInfo& attrs, const Attribute& a) {
  auto it = std::find_if(attrs.begin(), attrs.end(), [&](const AttributeInfo& ai) {
    return AttrMatches(ai, a);
  });
  if (it == attrs.end()) {
    throw std::runtime_error{"merge join key attribute not found: " + a.table + "." + a.name};
  }
  return static_cast<size_t>(it - attrs.begin());
}

struct ExecutorJoinKeys {
  Attribute lhs;
  Attribute rhs;
};

ExecutorJoinKeys ResolveExecutorJoinKeys(const Expression& qual,
                                         const AttributesInfo& lhs_attrs,
                                         const AttributesInfo& rhs_attrs) {
  const auto* bin = std::get_if<BinaryExpression>(&qual);
  if (!bin || bin->binop != BinaryOp::kEq) {
    throw std::logic_error{"MergeJoin qual must be an equality"};
  }
  const auto* a = std::get_if<Attribute>(bin->lhs.get());
  const auto* b = std::get_if<Attribute>(bin->rhs.get());
  if (!a || !b) {
    throw std::logic_error{"MergeJoin qual must be `attr = attr`"};
  }

  const bool a_lhs = HasAttr(lhs_attrs, *a);
  const bool a_rhs = HasAttr(rhs_attrs, *a);
  const bool b_lhs = HasAttr(lhs_attrs, *b);
  const bool b_rhs = HasAttr(rhs_attrs, *b);
  if (a_lhs && !a_rhs && b_rhs && !b_lhs) {
    return {*a, *b};
  }
  if (b_lhs && !b_rhs && a_rhs && !a_lhs) {
    return {*b, *a};
  }
  throw std::runtime_error{"MergeJoin qual cannot be mapped unambiguously to inputs"};
}

boost::asio::awaitable<Tuples> CollectAllTuples(TuplesChannel& tuples_chan) {
  Tuples result;
  for (;;) {
    auto buf = co_await ReceiveTuples(tuples_chan);
    if (buf.empty()) break;
    std::move(buf.begin(), buf.end(), std::back_inserter(result));
  }
  co_return result;
}

int CompareNonNullValues(const Value& lhs, const Value& rhs, Type type) {
  switch (type) {
    case Type::kInt:
      return (lhs.value.int_value > rhs.value.int_value)
           - (lhs.value.int_value < rhs.value.int_value);
    case Type::kBool:
      return (lhs.value.bool_value > rhs.value.bool_value)
           - (lhs.value.bool_value < rhs.value.bool_value);
    case Type::kString: {
      const auto& l = GetInternedString(lhs.value.string_id);
      const auto& r = GetInternedString(rhs.value.string_id);
      return (l > r) - (l < r);
    }
  }
  std::unreachable();
}

std::optional<BinaryExpression> FindHashJoinKey(const Expression& qual) {
  std::vector<Expression> conjuncts;
  CollectConjuncts(qual, conjuncts);
  for (const auto& conjunct : conjuncts) {
    const auto* bin = std::get_if<BinaryExpression>(&conjunct);
    if (!bin || bin->binop != BinaryOp::kEq) continue;
    if (!std::holds_alternative<Attribute>(*bin->lhs)
        || !std::holds_alternative<Attribute>(*bin->rhs)) {
      continue;
    }
    return *bin;
  }
  return std::nullopt;
}

} // namespace






template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteHashJoin(
    const HashJoin& join, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  Log("Executing hash join");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { attr_chan.close(); tuples_chan.close(); });
  if (join.type != JoinType::kInner) {
    throw std::logic_error{"HashJoin executor supports only Inner joins"};
  }
  const auto bin = FindHashJoinKey(join.qual);
  if (!bin) {
    throw std::logic_error{"HashJoin qual must contain an equality between attributes"};
  }
  const auto* a = std::get_if<Attribute>(bin->lhs.get());
  const auto* b = std::get_if<Attribute>(bin->rhs.get());

  auto exec = co_await boost::asio::this_coro::executor;
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  auto lhs_task = SpawnExecutor(exec, *join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  auto rhs_task = SpawnExecutor(exec, *join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto lhs_attrs = co_await lhs_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto rhs_attrs = co_await rhs_attrs_chan.async_receive(boost::asio::use_awaitable);

 
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
  if (GetExpressionType(join.qual, out_attrs) != Type::kBool) {
    throw std::logic_error{"hash join qual should return bool"};
  }
  auto qual_executor = co_await expression_executor_.GetExpressionExecutor(join.qual, out_attrs);

  co_await attr_chan.async_send(boost::system::error_code{}, out_attrs,
                                boost::asio::use_awaitable);
  attr_chan.close();

 
 
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
        auto joined_tuple = ConcatTuples(it->second, rt);
        if (!ApplyFilter(joined_tuple, out_attrs, qual_executor)) {
          continue;
        }
        out_buf.push_back(std::move(joined_tuple));
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
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteMergeJoin(
    const MergeJoin& join, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan) {
  Log("Executing merge join");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { attr_chan.close(); tuples_chan.close(); });

  auto exec = co_await boost::asio::this_coro::executor;
  auto [lhs_attrs_chan, lhs_tuples_chan] = co_await GetChannels();
  auto lhs_task = SpawnExecutor(exec, *join.lhs, lhs_attrs_chan, lhs_tuples_chan);
  auto [rhs_attrs_chan, rhs_tuples_chan] = co_await GetChannels();
  auto rhs_task = SpawnExecutor(exec, *join.rhs, rhs_attrs_chan, rhs_tuples_chan);

  auto lhs_attrs = co_await lhs_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto rhs_attrs = co_await rhs_attrs_chan.async_receive(boost::asio::use_awaitable);
  auto keys = ResolveExecutorJoinKeys(join.qual, lhs_attrs, rhs_attrs);
  const size_t lhs_key_idx = FindAttrIndexFlexible(lhs_attrs, keys.lhs);
  const size_t rhs_key_idx = FindAttrIndexFlexible(rhs_attrs, keys.rhs);
  if (lhs_attrs[lhs_key_idx].type != rhs_attrs[rhs_key_idx].type) {
    throw std::logic_error{"MergeJoin key types mismatch"};
  }
  const Type key_type = lhs_attrs[lhs_key_idx].type;

  AttributesInfo out_attrs = lhs_attrs;
  std::ranges::copy(rhs_attrs, std::back_inserter(out_attrs));
  co_await attr_chan.async_send(boost::system::error_code{}, out_attrs,
                                boost::asio::use_awaitable);
  attr_chan.close();

  auto lhs = co_await CollectAllTuples(lhs_tuples_chan);
  auto rhs = co_await CollectAllTuples(rhs_tuples_chan);

  Tuples out_buf;
  out_buf.reserve(kBufSize);
  auto emit = [&](Tuple tuple) -> boost::asio::awaitable<void> {
    out_buf.push_back(std::move(tuple));
    if (out_buf.size() == kBufSize) {
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                      boost::asio::use_awaitable);
      out_buf.clear();
      out_buf.reserve(kBufSize);
    }
  };
  auto flush = [&]() -> boost::asio::awaitable<void> {
    if (!out_buf.empty()) {
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                      boost::asio::use_awaitable);
      out_buf.clear();
      out_buf.reserve(kBufSize);
    }
  };

  auto same_lhs_key = [&](size_t a, size_t b) {
    const auto& av = lhs[a][lhs_key_idx];
    const auto& bv = lhs[b][lhs_key_idx];
    return !av.is_null && !bv.is_null && CompareNonNullValues(av, bv, key_type) == 0;
  };
  auto same_rhs_key = [&](size_t a, size_t b) {
    const auto& av = rhs[a][rhs_key_idx];
    const auto& bv = rhs[b][rhs_key_idx];
    return !av.is_null && !bv.is_null && CompareNonNullValues(av, bv, key_type) == 0;
  };

  if (join.type == JoinType::kRight) {
    size_t i = 0;
    size_t j = 0;
    while (j < rhs.size()) {
      const auto& rhs_key = rhs[j][rhs_key_idx];
      size_t rhs_end = j + 1;
      while (rhs_end < rhs.size() && same_rhs_key(j, rhs_end)) ++rhs_end;

      if (rhs_key.is_null) {
        for (size_t r = j; r < rhs_end; ++r) {
          co_await emit(ConcatTuples(Tuple(lhs_attrs.size(), Value{true}), rhs[r]));
        }
        j = rhs_end;
        continue;
      }

      while (i < lhs.size()) {
        const auto& lhs_key = lhs[i][lhs_key_idx];
        if (lhs_key.is_null || CompareNonNullValues(lhs_key, rhs_key, key_type) >= 0) break;
        ++i;
      }

      if (i < lhs.size() && !lhs[i][lhs_key_idx].is_null
          && CompareNonNullValues(lhs[i][lhs_key_idx], rhs_key, key_type) == 0) {
        size_t lhs_end = i + 1;
        while (lhs_end < lhs.size() && same_lhs_key(i, lhs_end)) ++lhs_end;
        for (size_t l = i; l < lhs_end; ++l) {
          for (size_t r = j; r < rhs_end; ++r) {
            co_await emit(ConcatTuples(lhs[l], rhs[r]));
          }
        }
        i = lhs_end;
      } else {
        for (size_t r = j; r < rhs_end; ++r) {
          co_await emit(ConcatTuples(Tuple(lhs_attrs.size(), Value{true}), rhs[r]));
        }
      }
      j = rhs_end;
    }
  } else {
    std::vector<char> rhs_used(rhs.size(), false);
    size_t i = 0;
    size_t j = 0;
    while (i < lhs.size()) {
      const auto& lhs_key = lhs[i][lhs_key_idx];
      size_t lhs_end = i + 1;
      while (lhs_end < lhs.size() && same_lhs_key(i, lhs_end)) ++lhs_end;

      if (lhs_key.is_null) {
        if (join.type == JoinType::kLeft || join.type == JoinType::kFull) {
          for (size_t l = i; l < lhs_end; ++l) {
            co_await emit(ConcatTuples(lhs[l], Tuple(rhs_attrs.size(), Value{true})));
          }
        }
        i = lhs_end;
        continue;
      }

      while (j < rhs.size()) {
        const auto& rhs_key = rhs[j][rhs_key_idx];
        if (rhs_key.is_null || CompareNonNullValues(rhs_key, lhs_key, key_type) >= 0) break;
        ++j;
      }

      if (j < rhs.size() && !rhs[j][rhs_key_idx].is_null
          && CompareNonNullValues(rhs[j][rhs_key_idx], lhs_key, key_type) == 0) {
        size_t rhs_end = j + 1;
        while (rhs_end < rhs.size() && same_rhs_key(j, rhs_end)) ++rhs_end;
        for (size_t l = i; l < lhs_end; ++l) {
          for (size_t r = j; r < rhs_end; ++r) {
            rhs_used[r] = true;
            co_await emit(ConcatTuples(lhs[l], rhs[r]));
          }
        }
        j = rhs_end;
      } else if (join.type == JoinType::kLeft || join.type == JoinType::kFull) {
        for (size_t l = i; l < lhs_end; ++l) {
          co_await emit(ConcatTuples(lhs[l], Tuple(rhs_attrs.size(), Value{true})));
        }
      }
      i = lhs_end;
    }

    if (join.type == JoinType::kFull) {
      for (size_t r = 0; r < rhs.size(); ++r) {
        if (!rhs_used[r]) {
          co_await emit(ConcatTuples(Tuple(lhs_attrs.size(), Value{true}), rhs[r]));
        }
      }
    }
  }

  co_await flush();
  co_await lhs_task(boost::asio::use_awaitable);
  co_await rhs_task(boost::asio::use_awaitable);
  tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteHashAggregate(
    const PhysicalAggregation& agg, AttributesInfoChannel& out_attr_chan,
    TuplesChannel& out_tuples_chan) {
  Log("Executing hash aggregate");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { out_attr_chan.close(); out_tuples_chan.close(); });
  auto exec = co_await boost::asio::this_coro::executor;
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, *agg.source, in_attrs_chan, in_tuples_chan);

  auto in_attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);

 
  AttributesInfo out_attrs;
  for (const auto& expr : agg.group_by) {
    if (const auto* attr = std::get_if<Attribute>(&expr)) {
      auto it = std::find_if(in_attrs.begin(), in_attrs.end(), [&](const AttributeInfo& ai) {
        return ai.table == attr->table && ai.name == attr->name;
      });
      if (it != in_attrs.end()) {
        out_attrs.push_back(*it);
      }
    }
  }
  for (size_t i = 0; i < agg.aggregates.size(); ++i) {
    const auto& agg_expr = std::get<AggregateExpression>(agg.aggregates[i]);
    Type t = (agg_expr.function == AggregateFunction::kCount)
                 ? Type::kInt
                 : (agg_expr.is_star ? Type::kInt : GetExpressionTypeUnchecked(*agg_expr.argument, in_attrs));
    out_attrs.push_back(AttributeInfo{"", std::format("__agg{}", i), t});
  }
  co_await out_attr_chan.async_send(boost::system::error_code{}, out_attrs,
                                   boost::asio::use_awaitable);
  out_attr_chan.close();

 
 
 
  struct GroupState {
    std::vector<int64_t> accumulators;
    std::vector<bool> any_non_null;
  };
  struct TupleKeyHash {
    size_t operator()(const std::vector<Value>& key) const {
      size_t seed = 0;
      for (const auto& v : key) {
        boost::hash_combine(seed, v.is_null);
        if (!v.is_null) boost::hash_combine(seed, v.value.int_value);
      }
      return seed;
    }
  };
  std::unordered_map<std::vector<Value>, GroupState, TupleKeyHash> groups;

 
  auto do_scalar = [&](const Expression& expr, const Tuple& tuple) -> Value {
    return CalcExpression(tuple, in_attrs, expr);
  };

  const bool scalar_agg = agg.group_by.empty();

  auto init_state = [&]() -> GroupState {
    GroupState s;
    s.accumulators.resize(agg.aggregates.size(), 0);
    s.any_non_null.resize(agg.aggregates.size(), false);
    return s;
  };

  for (;;) {
    auto buf = co_await ReceiveTuples(in_tuples_chan);
    if (buf.empty()) break;
    for (const auto& tuple : buf) {
      std::vector<Value> key;
      for (const auto& expr : agg.group_by) {
        key.push_back(do_scalar(expr, tuple));
      }
      auto [it, inserted] = groups.emplace(key, GroupState{});
      if (inserted) it->second = init_state();
      auto& state = it->second;
      for (size_t i = 0; i < agg.aggregates.size(); ++i) {
        const auto& agg_expr = std::get<AggregateExpression>(agg.aggregates[i]);
        if (agg_expr.function == AggregateFunction::kCount) {
          if (agg_expr.is_star) {
            state.accumulators[i]++;
          } else {
            auto v = do_scalar(*agg_expr.argument, tuple);
            if (!v.is_null) state.accumulators[i]++;
          }
        } else {
          auto v = do_scalar(*agg_expr.argument, tuple);
          if (!v.is_null) {
            state.accumulators[i] += v.value.int_value;
            state.any_non_null[i] = true;
          }
        }
      }
    }
  }

 
  if (scalar_agg && groups.empty()) {
    groups.emplace(std::vector<Value>{}, init_state());
  }

  Tuples out_buf;
  out_buf.reserve(kBufSize);
  for (auto& [key, state] : groups) {
    Tuple tuple = key;
    for (size_t i = 0; i < agg.aggregates.size(); ++i) {
      const auto& agg_expr = std::get<AggregateExpression>(agg.aggregates[i]);
      if (agg_expr.function == AggregateFunction::kSum && !state.any_non_null[i]) {
        tuple.push_back(Value{true});
      } else {
        tuple.push_back(Value{false, {.int_value = state.accumulators[i]}});
      }
    }
    out_buf.push_back(std::move(tuple));
    if (out_buf.size() == kBufSize) {
      co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                          boost::asio::use_awaitable);
      out_buf.clear();
      out_buf.reserve(kBufSize);
    }
  }
  if (!out_buf.empty()) {
    co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                        boost::asio::use_awaitable);
  }
  co_await task(boost::asio::use_awaitable);
  out_tuples_chan.close();
}

template <typename ExpressionExecutor>
boost::asio::awaitable<void> Executor<ExpressionExecutor>::ExecuteStreamAggregate(
    const PhysicalStreamAggregation& agg, AttributesInfoChannel& out_attr_chan,
    TuplesChannel& out_tuples_chan) {
  Log("Executing stream aggregate");
  auto close_on_fail = boost::scope::make_scope_fail(
      [&] { out_attr_chan.close(); out_tuples_chan.close(); });
  auto exec = co_await boost::asio::this_coro::executor;
  auto [in_attrs_chan, in_tuples_chan] = co_await GetChannels();
  auto task = SpawnExecutor(exec, *agg.source, in_attrs_chan, in_tuples_chan);

  auto in_attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);

  AttributesInfo out_attrs;
  for (const auto& expr : agg.group_by) {
    if (const auto* attr = std::get_if<Attribute>(&expr)) {
      auto it = std::find_if(in_attrs.begin(), in_attrs.end(), [&](const AttributeInfo& ai) {
        return ai.table == attr->table && ai.name == attr->name;
      });
      if (it != in_attrs.end()) {
        out_attrs.push_back(*it);
      }
    }
  }
  for (size_t i = 0; i < agg.aggregates.size(); ++i) {
    const auto& agg_expr = std::get<AggregateExpression>(agg.aggregates[i]);
    Type t = (agg_expr.function == AggregateFunction::kCount)
                 ? Type::kInt
                 : (agg_expr.is_star ? Type::kInt : GetExpressionTypeUnchecked(*agg_expr.argument, in_attrs));
    out_attrs.push_back(AttributeInfo{"", std::format("__agg{}", i), t});
  }
  co_await out_attr_chan.async_send(boost::system::error_code{}, out_attrs,
                                    boost::asio::use_awaitable);
  out_attr_chan.close();

  struct GroupState {
    std::vector<int64_t> accumulators;
    std::vector<bool> any_non_null;
  };

  auto do_scalar = [&](const Expression& expr, const Tuple& tuple) -> Value {
    return CalcExpression(tuple, in_attrs, expr);
  };
  auto init_state = [&]() -> GroupState {
    GroupState s;
    s.accumulators.resize(agg.aggregates.size(), 0);
    s.any_non_null.resize(agg.aggregates.size(), false);
    return s;
  };
  auto update_state = [&](GroupState& state, const Tuple& tuple) {
    for (size_t i = 0; i < agg.aggregates.size(); ++i) {
      const auto& agg_expr = std::get<AggregateExpression>(agg.aggregates[i]);
      if (agg_expr.function == AggregateFunction::kCount) {
        if (agg_expr.is_star) {
          state.accumulators[i]++;
        } else {
          auto v = do_scalar(*agg_expr.argument, tuple);
          if (!v.is_null) state.accumulators[i]++;
        }
      } else {
        auto v = do_scalar(*agg_expr.argument, tuple);
        if (!v.is_null) {
          state.accumulators[i] += v.value.int_value;
          state.any_non_null[i] = true;
        }
      }
    }
  };

  Tuples out_buf;
  out_buf.reserve(kBufSize);
  auto emit_group = [&](const std::vector<Value>& key, const GroupState& state)
      -> boost::asio::awaitable<void> {
    Tuple tuple = key;
    for (size_t i = 0; i < agg.aggregates.size(); ++i) {
      const auto& agg_expr = std::get<AggregateExpression>(agg.aggregates[i]);
      if (agg_expr.function == AggregateFunction::kSum && !state.any_non_null[i]) {
        tuple.push_back(Value{true});
      } else {
        tuple.push_back(Value{false, {.int_value = state.accumulators[i]}});
      }
    }
    out_buf.push_back(std::move(tuple));
    if (out_buf.size() == kBufSize) {
      co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                          boost::asio::use_awaitable);
      out_buf.clear();
      out_buf.reserve(kBufSize);
    }
  };
  auto flush_output = [&]() -> boost::asio::awaitable<void> {
    if (!out_buf.empty()) {
      co_await out_tuples_chan.async_send(boost::system::error_code{}, std::move(out_buf),
                                          boost::asio::use_awaitable);
      out_buf.clear();
      out_buf.reserve(kBufSize);
    }
  };

  std::vector<Value> current_key;
  GroupState current_state = init_state();
  bool has_current = false;

  for (;;) {
    auto buf = co_await ReceiveTuples(in_tuples_chan);
    if (buf.empty()) break;
    for (const auto& tuple : buf) {
      std::vector<Value> key;
      key.reserve(agg.group_by.size());
      for (const auto& expr : agg.group_by) {
        key.push_back(do_scalar(expr, tuple));
      }
      if (!has_current) {
        current_key = std::move(key);
        current_state = init_state();
        has_current = true;
      } else if (key != current_key) {
        co_await emit_group(current_key, current_state);
        current_key = std::move(key);
        current_state = init_state();
      }
      update_state(current_state, tuple);
    }
  }

  if (has_current) {
    co_await emit_group(current_key, current_state);
  } else if (agg.group_by.empty()) {
    co_await emit_group(std::vector<Value>{}, init_state());
  }
  co_await flush_output();
  co_await task(boost::asio::use_awaitable);
  out_tuples_chan.close();
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
