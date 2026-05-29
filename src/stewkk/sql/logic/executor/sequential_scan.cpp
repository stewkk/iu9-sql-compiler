#include <stewkk/sql/logic/executor/sequential_scan.hpp>

#include <fstream>
#include <format>
#include <ranges>
#include <iostream>
#include <stdexcept>
#include <string_view>

#include <boost/asio/use_awaitable.hpp>

#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>

namespace stewkk::sql {

namespace {

Type GetTypeFromString(const std::string& s) {
    if (s == "int") {
        return Type::kInt;
    }
    if (s == "string") {
        return Type::kString;
    }
    std::unreachable();
}

Value BuildValueFromString(Type type, const std::string& value) {
  if (value == "NULL") {
    return Value{true};
  }
  switch (type) {
    case Type::kInt:
      return Value{false, std::stoi(value)};
    case Type::kString:
      return Value{false, {.string_id = InternString(value)}};
    default:
      std::unreachable();
  }
}

std::vector<std::string> ParseCsvLine(const std::string& line) {
  auto input = std::string_view{line};
  if (!input.empty() && input.back() == '\r') {
    input.remove_suffix(1);
  }
  std::vector<std::string> fields;
  std::string field;
  bool in_quotes = false;
  for (size_t i = 0; i < input.size(); ++i) {
    char c = input[i];
    if (in_quotes) {
      if (c == '"') {
        if (i + 1 < input.size() && input[i + 1] == '"') {
          field.push_back('"');
          ++i;
        } else {
          in_quotes = false;
        }
      } else {
        field.push_back(c);
      }
    } else if (c == '"') {
      in_quotes = true;
    } else if (c == ',') {
      fields.push_back(std::move(field));
      field.clear();
    } else {
      field.push_back(c);
    }
  }
  fields.push_back(std::move(field));
  return fields;
}

Tuple ParseTuple(const std::string& line,
                 const AttributesInfo& attributes) {
  auto fields = ParseCsvLine(line);
  if (fields.size() != attributes.size()) {
    throw std::runtime_error{std::format("CSV row has {} fields, expected {}", fields.size(), attributes.size())};
  }
  return fields | std::views::enumerate
         | std::views::transform([&attributes](const auto& attr) {
             const auto& [index, value] = attr;
             const auto& [_, attr_name, type] = attributes[index];
             return BuildValueFromString(type, value);
           })
         | std::ranges::to<Tuple>();
}

} // namespace

boost::asio::awaitable<Result<>> CsvDirSequentialScanner::operator()(
    const std::string& table_name, const std::string& output_table_name,
    AttributesInfoChannel& attrs_chan,
    TuplesChannel& tuples_chan) const {
#ifdef DEBUG
  std::clog << "Executing sequential scan\n";
#endif
  auto path = std::format("{}/{}.csv", dir, table_name);
  std::ifstream input{std::move(path)};
  std::string line;
  std::getline(input, line);
  auto attributes = line | std::views::split(',') | std::views::transform([&output_table_name](const auto& attr) {
                      auto mid = std::find(attr.begin(), attr.end(), ':');
                      auto attr_name = std::string{attr.begin(), mid};
                      auto attr_type = GetTypeFromString(std::string{mid + 1, attr.end()});
                      return AttributeInfo{output_table_name, std::move(attr_name), attr_type};
                    })
                    | std::ranges::to<AttributesInfo>();

  co_await attrs_chan.async_send(boost::system::error_code{}, attributes,
                               boost::asio::use_awaitable);
  attrs_chan.close();

  Tuples buf;
  buf.reserve(kBufSize);
  while (std::getline(input, line)) {
    auto tuple = ParseTuple(line, attributes);
    buf.emplace_back(std::move(tuple));
#ifdef DEBUG
    std::clog << std::format("buf size is {}\n", buf.size());
#endif

    if (buf.size() == kBufSize) {
#ifdef DEBUG
      std::clog << std::format("Sending {} tuples from table\n", buf.size());
#endif
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                               boost::asio::use_awaitable);
      buf.clear();
    }
  }

  if (!buf.empty()) {
#ifdef DEBUG
    std::clog << std::format("Sending {} tuples from table\n", buf.size());
#endif
    co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                             boost::asio::use_awaitable);
  }

  tuples_chan.close();
#ifdef DEBUG
  std::clog << "Done sending tuples from table\n";
#endif

  co_return Ok();
}

}  // namespace stewkk::sql
