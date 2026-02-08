#pragma once

#include <boost/asio/experimental/concurrent_channel.hpp>

#include <stewkk/sql/models/executor/tuple.hpp>

namespace stewkk::sql {

using TuplesChannel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code, Tuples)>;
using AttributesInfoChannel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code, AttributesInfo)>;

}  // namespace stewkk::sql
