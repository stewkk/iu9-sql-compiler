#pragma once

#include <string>
#include <functional>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/channel.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

class Executor {
public:
  using SequentialScan = std::function<boost::asio::awaitable<Result<>>(
      const std::string& table_name, AttributesInfoChannel& attr_chan, TuplesChannel& tuples_chan)>;
  explicit Executor(SequentialScan seq_scan);

  boost::asio::awaitable<Result<Relation>> Execute(const Operator& op) const;
private:
  boost::asio::awaitable<void> Execute(const Operator& op, AttributesInfoChannel& attr_chan,
                                       TuplesChannel& tuples_chan) const;
  boost::asio::awaitable<void> ExecuteProjection(const Projection& proj, AttributesInfoChannel& attr_chan,
                                                 TuplesChannel& tuples_chan) const;
  boost::asio::awaitable<void> ExecuteFilter(const Filter& filter, AttributesInfoChannel& attr_chan,
                                             TuplesChannel& tuples_chan) const;

private:
  SequentialScan sequential_scan_;
};

}  // namespace stewkk::sql
