#include <stewkk/sql/logic/executor/sequential_scan.hpp>

#include <fstream>
#include <ranges>

#include <stewkk/sql/models/executor/tuple.hpp>

namespace stewkk::sql {

constexpr static std::size_t kBufSize = 10;

namespace {

enum class Type {
     kInt,
};

Type GetTypeFromString(const std::string& s) {
    if (s == "int") {
        return Type::kInt;
    }
    std::unreachable();
}

AttributeValue BuildAttributeValueFromString(Type type, const std::string& table, const std::string attr_name, const std::string& value) {
  switch (type) {
    case Type::kInt:
      return AttributeValue{table, attr_name, std::stoi(value)};
    default:
      std::unreachable();
  }
}

Tuple ParseTuple(const std::string& line,
                 const std::vector<std::pair<std::string, Type>>& attributes,
                 const std::string& table_name) {
  return line | std::views::split(',') | std::views::enumerate
         | std::views::transform([&attributes, &table_name](const auto& attr) {
             const auto& [index, value_range] = attr;
             auto value = value_range | std::ranges::to<std::string>();
             const auto& [attr_name, type] = attributes[index];
             return BuildAttributeValueFromString(type, table_name, attr_name, value);
           })
         | std::ranges::to<std::vector>();
}

} // namespace

boost::asio::awaitable<Result<>> CsvDirSequentialScanner::operator()(const std::string& table_name, Channel& chan) const {
    auto path = std::format("{}/{}.csv", dir, table_name);
    std::ifstream input{std::move(path)};
    std::string line;
    std::getline(input, line);
    auto attributes
        = line | std::views::split(',') | std::views::transform([](const auto& attr) {
            auto mid = std::find(attr.begin(), attr.end(), ':');
            return std::make_pair(std::string{attr.begin(), mid}, GetTypeFromString(std::string{mid + 1, attr.end()}));
          })
          | std::ranges::to<std::vector>();

    std::vector<Tuple> buf;
    buf.reserve(kBufSize);
    while (std::getline(input, line)) {
      auto tuple = ParseTuple(line, attributes, table_name);
      buf.emplace_back(std::move(tuple));

      if (buf.size() == buf.capacity()) {
          co_await chan.async_send(boost::system::error_code{}, std::move(buf), boost::asio::use_awaitable);
          buf.clear();
      }
    }

    if (!buf.empty()) {
      co_await chan.async_send(boost::system::error_code{}, std::move(buf), boost::asio::use_awaitable);
    }

    co_return Ok();
}

}  // namespace stewkk::sql
