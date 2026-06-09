#pragma once

#include <boost/asio/awaitable.hpp>

#include <optional>

#include <stewkk/sql/logic/executor/channel.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>
#include <stewkk/sql/logic/result/result.hpp>

namespace stewkk::sql {

struct CsvDirSequentialScanner {
    std::string dir;

    boost::asio::awaitable<Result<>> operator()(const std::string& table_name,
                                                const std::string& output_table_name,
                                                AttributesInfoChannel& attrs_chan,
                                                TuplesChannel& tuples_chan) const;
};

struct CsvDirIndexedScanner {
    std::string dir;

    boost::asio::awaitable<Result<>> operator()(const std::string& table_name,
                                                const std::string& output_table_name,
                                                const Expression& predicate,
                                                const std::optional<std::string>& index_column,
                                                AttributesInfoChannel& attrs_chan,
                                                TuplesChannel& tuples_chan) const;
};

}  // namespace stewkk::sql
