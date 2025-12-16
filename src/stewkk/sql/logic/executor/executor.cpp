#include <stewkk/sql/logic/executor/executor.hpp>

#include <algorithm>
#include <ranges>
#include <cmath>
#include <iostream>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

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
      if (lhs_type != Type::kBool) {
        throw std::logic_error{"types mismatch"};
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

template <typename Op>
  requires std::invocable<Op, int64_t, int64_t>
           && std::same_as<std::invoke_result_t<Op, int64_t, int64_t>, int64_t>
Value ApplyBinaryIntegerOperator(Value lhs, Value rhs) {
  if (std::get_if<NullValue>(&lhs) || std::get_if<NullValue>(&rhs)) {
    return NullValue{};
  }
  auto lhs_value = std::get<NonNullValue>(std::move(lhs));
  auto rhs_value = std::get<NonNullValue>(std::move(rhs));
  return NonNullValue{Op{}(lhs_value.int_value, rhs_value.int_value)};
}

template <typename Op>
  requires std::invocable<Op, bool, bool>
           && std::same_as<std::invoke_result_t<Op, bool, bool>, bool>
Value ApplyBinaryBooleanOperator(Value lhs, Value rhs) {
  if (std::get_if<NullValue>(&lhs) || std::get_if<NullValue>(&rhs)) {
    return GetTrileanValue(Trilean::kUnknown);
  }
  auto lhs_value = std::get<NonNullValue>(std::move(lhs));
  auto rhs_value = std::get<NonNullValue>(std::move(rhs));
  bool lhs_bool;
  switch (lhs_value.trilean_value) {
    case Trilean::kTrue:
      lhs_bool = true;
    case Trilean::kFalse:
      lhs_bool = false;
    case Trilean::kUnknown:
      return GetTrileanValue(Trilean::kUnknown);
  }
  bool rhs_bool;
  switch (rhs_value.trilean_value) {
    case Trilean::kTrue:
      rhs_bool = true;
    case Trilean::kFalse:
      rhs_bool = false;
    case Trilean::kUnknown:
      return GetTrileanValue(Trilean::kUnknown);
  }
  if (Op{}(lhs_bool, rhs_bool)) {
    return GetTrileanValue(Trilean::kTrue);
  }
  return GetTrileanValue(Trilean::kFalse);
}

struct IntPow {
  int64_t operator()(int64_t base, int64_t exp) const {
    return static_cast<int64_t>(std::pow(base, exp));
  }
};

Value ApplyProjection(const Tuple& source, const AttributesInfo& source_attrs, const Expression& expr) {
  struct ExpressionVisitor {
    Value operator()(const BinaryExpression& expr) {
      auto lhs = std::visit(*this, *expr.lhs);
      auto rhs = std::visit(*this, *expr.rhs);
      switch (expr.binop) {
        case BinaryOp::kGt:
          return ApplyBinaryBooleanOperator<std::greater<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kLt:
          return ApplyBinaryBooleanOperator<std::less<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kLe:
          return ApplyBinaryBooleanOperator<std::less_equal<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kGe:
          return ApplyBinaryBooleanOperator<std::greater_equal<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kNotEq:
          return ApplyBinaryBooleanOperator<std::not_equal_to<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kEq:
          return ApplyBinaryBooleanOperator<std::equal_to<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kOr:
          return ApplyBinaryBooleanOperator<std::logical_or<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kAnd:
          return ApplyBinaryBooleanOperator<std::logical_and<void>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kPlus:
          return ApplyBinaryIntegerOperator<std::plus<int64_t>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kMinus:
          return ApplyBinaryIntegerOperator<std::minus<int64_t>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kMul:
          return ApplyBinaryIntegerOperator<std::multiplies<int64_t>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kDiv:
          return ApplyBinaryIntegerOperator<std::divides<int64_t>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kMod:
          return ApplyBinaryIntegerOperator<std::modulus<int64_t>>(std::move(lhs), std::move(rhs));
        case BinaryOp::kPow:
          return ApplyBinaryIntegerOperator<IntPow>(std::move(lhs), std::move(rhs));
      }
    }
    Value operator()(const UnaryExpression& expr) {
      auto child = std::visit(*this, *expr.child);

      switch (expr.op) {
        case UnaryOp::kNot:
          struct NotVisitor {
            Value operator()(const NullValue& n) {
              return GetTrileanValue(Trilean::kUnknown);
            }
            Value operator()(const NonNullValue& val) {
              switch (val.trilean_value) {
                case Trilean::kTrue:
                  return  GetTrileanValue(Trilean::kFalse);
                case Trilean::kFalse:
                  return  GetTrileanValue(Trilean::kTrue);
                case Trilean::kUnknown:
                  return  GetTrileanValue(Trilean::kUnknown);
              }
            }
          };
          return std::visit(NotVisitor{}, child);
        case UnaryOp::kMinus:
          struct MinusVisitor {
            Value operator()(const NullValue& n) {
              return NullValue{};
            }
            Value operator()(const NonNullValue& val) {
              return NonNullValue{-val.int_value};
            }
          };
          return std::visit(MinusVisitor{}, child);
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
      return NonNullValue{expr};
    }
    Value operator()(const Literal& expr) {
      switch (expr) {
        case Literal::kNull:
          return NullValue{};
        case Literal::kTrue: {
          return GetTrileanValue(Trilean::kTrue);
        }
        case Literal::kFalse: {
          return GetTrileanValue(Trilean::kFalse);
        }
        case Literal::kUnknown: {
          return GetTrileanValue(Trilean::kUnknown);
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
    return ApplyProjection(source, source_attrs, target);
  }) | std::ranges::to<Tuple>();
}

} // namespace

Executor::Executor(SequentialScan seq_scan)
    : sequential_scan_(std::move(seq_scan)) {}

boost::asio::awaitable<Result<Relation>> Executor::Execute(const Operator& op) const {
  auto executor = co_await boost::asio::this_coro::executor;
  AttributesInfoChannel attr_chan{executor, 1};
  TuplesChannel tuples_chan{executor, 1};
  boost::asio::co_spawn(executor, Execute(op, attr_chan, tuples_chan), boost::asio::detached);

  auto attrs = co_await attr_chan.async_receive(boost::asio::use_awaitable);
  std::clog << "Received attrs in root\n";

  Tuples result;
  for (;;) {
    Tuples buf;
    try {
      buf = co_await tuples_chan.async_receive(boost::asio::use_awaitable);
    } catch (const boost::system::system_error& ex) {
      break;
    }
    std::clog << std::format("Received {} tuples in root\n", buf.size());

    std::copy(buf.begin(), buf.end(), std::back_inserter(result));
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
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Join& join) {
      co_return;
    }
    boost::asio::awaitable<void> operator()(const CrossJoin& cross_join) {
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
  auto executor = co_await boost::asio::this_coro::executor;
  AttributesInfoChannel in_attrs_chan{executor, 1};
  TuplesChannel in_tuples_chan{executor, 1};
  boost::asio::co_spawn(executor, Execute(*proj.source, in_attrs_chan, in_tuples_chan),
                        boost::asio::detached);

  auto attrs = co_await in_attrs_chan.async_receive(boost::asio::use_awaitable);

  auto attrs_after = GetAttributesAfterProjection(attrs, proj);
  co_await out_attr_chan.async_send(boost::system::error_code{}, attrs_after,
                                    boost::asio::use_awaitable);
  out_attr_chan.close();

  for (;;) {
    Tuples buf;
    try {
      buf = co_await in_tuples_chan.async_receive(boost::asio::use_awaitable);
    } catch (const boost::system::system_error& ex) {
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

}  // namespace stewkk::sql
