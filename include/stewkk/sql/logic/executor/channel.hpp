#pragma once

#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>

#include <stewkk/sql/models/executor/tuple.hpp>

namespace stewkk::sql {

using Channel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code, std::vector<Tuple>)>;

}  // namespace stewkk::sql
