#pragma once

#include <string>
#include <functional>
#include <unordered_map>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/promise.hpp>
#include <boost/asio/experimental/use_promise.hpp>

#include <stewkk/sql/logic/executor/materialization.hpp>
#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/channel.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>
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

class CachedJitCompiledExpressionExecutor {
  public:
    explicit CachedJitCompiledExpressionExecutor(boost::asio::any_io_executor executor);
    boost::asio::awaitable<ExecExpression> GetExpressionExecutor(const Expression& expr, const AttributesInfo& attrs);
  private:
    JITCompiler compiler_;
    std::unordered_map<std::string, ExecExpression> cache_;
};

template <typename ExpressionExecutor = InterpretedExpressionExecutor>
class Executor {
public:
  using SequentialScan = std::function<boost::asio::awaitable<Result<>>(
      const std::string& table_name, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan)>;
  Executor(SequentialScan seq_scan, boost::asio::any_io_executor executor);

  boost::asio::awaitable<Result<Relation>> Execute(const PhysicalPlanNode& op);
private:
  boost::asio::awaitable<void> Execute(const PhysicalPlanNode& op, AttributesInfoChannel& attr_chan,
                                       TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteProjection(const PhysicalProjection& proj, AttributesInfoChannel& attr_chan,
                                                 TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteFilter(const PhysicalFilter& filter, AttributesInfoChannel& attr_chan,
                                             TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteCrossJoin(const NestedLoopCrossJoin& cross_join,
                                                AttributesInfoChannel& attr_chan,
                                                TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteJoin(const NestedLoopJoin& join, AttributesInfoChannel& attr_chan,
                                           TuplesChannel& tuples_chan);
  boost::asio::awaitable<void> ExecuteHashJoin(const HashJoin& join, AttributesInfoChannel& attr_chan,
                                                TuplesChannel& tuples_chan);
  boost::asio::experimental::promise<void(std::exception_ptr)> SpawnExecutor(
      boost::asio::any_io_executor exec,
      const PhysicalPlanNode& op, AttributesInfoChannel& attr_chan, TuplesChannel& tuple_chan);

private:
  SequentialScan sequential_scan_;
  ExpressionExecutor expression_executor_;
};

Type GetExpressionType(const Expression& expr, const AttributesInfo& available_attrs);
Type GetExpressionTypeUnchecked(const Expression& expr, const AttributesInfo& available_attrs);
AttributesInfo GetAttributesAfterProjection(const AttributesInfo& attrs, const PhysicalProjection& proj);

Value CalcExpression(const Tuple& source, const AttributesInfo& source_attrs, const Expression& expr);
Tuple ApplyProjection(const Tuple& source, const AttributesInfo& source_attrs, const std::vector<ExecExpression>& expressions);
bool ApplyFilter(const Tuple& source, const AttributesInfo& source_attrs, const ExecExpression& filter);
boost::asio::awaitable<std::pair<AttributesInfoChannel, TuplesChannel>> GetChannels();
boost::asio::awaitable<Tuples> ReceiveTuples(TuplesChannel& chan);
boost::asio::awaitable<AttributesInfo> ConcatAttrs(AttributesInfoChannel& lhs_attrs_chan, AttributesInfoChannel& rhs_attrs_chan);
boost::asio::awaitable<DiskFileReader> MaterializeChannel(TuplesChannel& tuples_chan);
Tuple ConcatTuples(const Tuple& lhs, const Tuple& rhs);

}  // namespace stewkk::sql
