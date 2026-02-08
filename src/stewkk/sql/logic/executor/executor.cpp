#include <stewkk/sql/logic/executor/executor.hpp>

namespace stewkk::sql {

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

}  // namespace stewkk::sql
