#pragma once

#include <boost/asio/awaitable.hpp>

#include <stewkk/sql/logic/executor/channel.hpp>
#include <stewkk/sql/logic/result/result.hpp>

namespace stewkk::sql {

struct CsvDirSequentialScanner {
    std::string dir;

    boost::asio::awaitable<Result<>> operator()(const std::string& table_name, Channel& chan) const;
};

}  // namespace stewkk::sql
