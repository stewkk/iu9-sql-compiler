#include <stewkk/sql/logic/executor/sequential_scan.hpp>

#include <fstream>
#include <ranges>
#include <iostream>

#include <boost/asio/use_awaitable.hpp>

#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>

namespace stewkk::sql {

namespace {

Type GetTypeFromString(const std::string& s) {
    if (s == "int") {
        return Type::kInt;
    }
    std::unreachable();
}

Value BuildValueFromString(Type type, const std::string& table, const std::string attr_name, const std::string& value) {
  if (value == "NULL") {
    return Value{true};
  }
  switch (type) {
    case Type::kInt:
      return Value{false, std::stoi(value)};
    default:
      std::unreachable();
  }
}

Tuple ParseTuple(const std::string& line,
                 const AttributesInfo& attributes,
                 const std::string& table_name) {
  return line | std::views::split(',') | std::views::enumerate
         | std::views::transform([&attributes, &table_name](const auto& attr) {
             const auto& [index, value_range] = attr;
             auto value = value_range | std::ranges::to<std::string>();
             const auto& [_, attr_name, type] = attributes[index];
             return BuildValueFromString(type, table_name, attr_name, value);
           })
         | std::ranges::to<std::vector>();
}

} // namespace

boost::asio::awaitable<Result<>> CsvDirSequentialScanner::operator()(
    const std::string& table_name, AttributesInfoChannel& attrs_chan,
    TuplesChannel& tuples_chan) const {
  std::clog << "Executing sequential scan\n";
  auto path = std::format("{}/{}.csv", dir, table_name);
  std::ifstream input{std::move(path)};
  std::string line;
  std::getline(input, line);
  auto attributes = line | std::views::split(',') | std::views::transform([&table_name](const auto& attr) {
                      auto mid = std::find(attr.begin(), attr.end(), ':');
                      auto attr_name = std::string{attr.begin(), mid};
                      auto attr_type = GetTypeFromString(std::string{mid + 1, attr.end()});
                      return AttributeInfo{table_name, std::move(attr_name), attr_type};
                    })
                    | std::ranges::to<AttributesInfo>();

  co_await attrs_chan.async_send(boost::system::error_code{}, attributes,
                               boost::asio::use_awaitable);
  attrs_chan.close();

  Tuples buf;
  buf.reserve(kBufSize);
  while (std::getline(input, line)) {
    auto tuple = ParseTuple(line, attributes, table_name);
    buf.emplace_back(std::move(tuple));
    std::clog << std::format("buf size is {}\n", buf.size());

    if (buf.size() == kBufSize) {
      std::clog << std::format("Sending {} tuples from table\n", buf.size());
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                               boost::asio::use_awaitable);
      buf.clear();
    }
  }

  if (!buf.empty()) {
    std::clog << std::format("Sending {} tuples from table\n", buf.size());
    co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                             boost::asio::use_awaitable);
  }

  tuples_chan.close();
  std::clog << "Done sending tuples from table\n";

  co_return Ok();
}

}  // namespace stewkk::sql
