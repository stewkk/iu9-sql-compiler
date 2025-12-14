#pragma once

#include <string>
#include <functional>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/channel.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

class Executor {
public:
    using SequentialScan = std::function<boost::asio::awaitable<Result<>>(const std::string& table_name, Channel& chan)>;
    Executor(boost::asio::any_io_executor executor, SequentialScan seq_scan);

    boost::asio::awaitable<Result<std::vector<Tuple>>> Execute(const Operator& op) const;
private:
    boost::asio::awaitable<void> Execute(const Operator& op, Channel& chan) const;

private:
     SequentialScan sequential_scan_;
     boost::asio::any_io_executor executor_;
};

}  // namespace stewkk::sql
