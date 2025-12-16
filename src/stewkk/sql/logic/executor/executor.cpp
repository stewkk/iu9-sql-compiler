#include <stewkk/sql/logic/executor/executor.hpp>

namespace stewkk::sql {

Executor::Executor(boost::asio::any_io_executor executor, SequentialScan seq_scan)
    : executor_(std::move(executor)), sequential_scan_(std::move(seq_scan)) {}

boost::asio::awaitable<Result<std::vector<Tuple>>> Executor::Execute(const Operator& op) const {
  Channel chan{executor_, 1};
  boost::asio::co_spawn(executor_, Execute(op, chan), boost::asio::detached);

  std::vector<Tuple> result;
  while (chan.is_open()) {
    auto buf = co_await chan.async_receive(boost::asio::use_awaitable);
    std::copy(buf.begin(), buf.end(), std::back_inserter(result));
  }

  co_return Ok(std::move(result));
}

boost::asio::awaitable<void> Executor::Execute(const Operator& op, Channel& chan) const {
  struct ExecuteVisitor{
    boost::asio::awaitable<void> operator()(const Table& table) {
      co_await executor.sequential_scan_(table.name, chan);
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Projection& table) {
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Filter& table) {
      co_return;
    }
    boost::asio::awaitable<void> operator()(const Join& table) {
      co_return;
    }
    boost::asio::awaitable<void> operator()(const CrossJoin& table) {
      co_return;
    }

    Channel& chan;
    const Executor& executor;
  };
  co_await std::visit(ExecuteVisitor{chan, *this}, op);
  co_return;
}

}  // namespace stewkk::sql
