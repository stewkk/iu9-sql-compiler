#include <stewkk/sql/logic/executor/sequential_scan.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <format>
#include <ranges>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <tuple>

#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>

#include <stewkk/sql/logic/executor/executor.hpp>
#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>

namespace stewkk::sql {

namespace {

constexpr std::array<char, 8> kIntSortedIndexMagic{'I', '9', 'I', 'D', 'X', '0', '0', '1'};

struct IndexMeta {
  std::string table;
  std::string column;
  std::string type;
  std::string file;
};

struct IntIndexEntry {
  int64_t key;
  uint64_t row_offset;
};

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

AttributesInfo ReadAttributes(std::istream& input, const std::string& output_table_name) {
  std::string line;
  if (!std::getline(input, line)) {
    throw std::runtime_error{"CSV file is empty"};
  }
  return line | std::views::split(',') | std::views::transform([&output_table_name](const auto& attr) {
                      auto mid = std::find(attr.begin(), attr.end(), ':');
                      auto attr_name = std::string{attr.begin(), mid};
                      auto attr_type = GetTypeFromString(std::string{mid + 1, attr.end()});
                      return AttributeInfo{output_table_name, std::move(attr_name), attr_type};
                    })
                    | std::ranges::to<AttributesInfo>();
}

std::vector<IndexMeta> ReadIndexMeta(const std::filesystem::path& path) {
  std::ifstream input{path};
  if (!input) {
    throw std::runtime_error{std::format("index metadata file not found: {}", path.string())};
  }

  std::vector<IndexMeta> result;
  std::string line;
  while (std::getline(input, line)) {
    auto first = line.find_first_not_of(" \t\r\n");
    if (first == std::string::npos || line[first] == '#') {
      continue;
    }

    std::istringstream fields{line};
    IndexMeta meta;
    if (!(fields >> meta.table >> meta.column >> meta.type >> meta.file)) {
      throw std::runtime_error{std::format("malformed index metadata line: {}", line)};
    }
    result.push_back(std::move(meta));
  }
  return result;
}

std::optional<IndexMeta> FindIndexMeta(const std::filesystem::path& dir,
                                       const std::string& table,
                                       const std::string& column) {
  for (auto meta : ReadIndexMeta(dir / "indexes.meta")) {
    if (meta.table == table && meta.column == column && meta.type == "sorted") {
      return meta;
    }
  }
  return std::nullopt;
}

BinaryOp InvertComparison(BinaryOp op) {
  switch (op) {
    case BinaryOp::kGt:
      return BinaryOp::kLt;
    case BinaryOp::kLt:
      return BinaryOp::kGt;
    case BinaryOp::kLe:
      return BinaryOp::kGe;
    case BinaryOp::kGe:
      return BinaryOp::kLe;
    case BinaryOp::kEq:
      return BinaryOp::kEq;
    default:
      throw std::runtime_error{"unsupported index seek operator"};
  }
}

struct SeekCondition {
  std::string column;
  BinaryOp op;
  int64_t key;
};

std::optional<SeekCondition> ExtractSeekCondition(const Expression& predicate,
                                                  const std::string& table_name,
                                                  const std::string& output_table_name) {
  const auto* binary = std::get_if<BinaryExpression>(&predicate);
  if (!binary) {
    return std::nullopt;
  }
  if (binary->binop == BinaryOp::kAnd) {
    if (auto lhs = ExtractSeekCondition(*binary->lhs, table_name, output_table_name)) {
      return lhs;
    }
    return ExtractSeekCondition(*binary->rhs, table_name, output_table_name);
  }
  if (!std::ranges::contains(std::vector{BinaryOp::kEq, BinaryOp::kLt, BinaryOp::kLe,
                                         BinaryOp::kGt, BinaryOp::kGe}, binary->binop)) {
    return std::nullopt;
  }

  auto attr_matches = [&](const Attribute& attr) {
    return attr.table.empty() || attr.table == table_name || attr.table == output_table_name;
  };
  if (const auto* attr = std::get_if<Attribute>(binary->lhs.get());
      attr && attr_matches(*attr)) {
    if (const auto* key = std::get_if<IntConst>(binary->rhs.get())) {
      return SeekCondition{attr->name, binary->binop, *key};
    }
  }
  if (const auto* attr = std::get_if<Attribute>(binary->rhs.get());
      attr && attr_matches(*attr)) {
    if (const auto* key = std::get_if<IntConst>(binary->lhs.get())) {
      return SeekCondition{attr->name, InvertComparison(binary->binop), *key};
    }
  }
  return std::nullopt;
}

std::vector<IntIndexEntry> ReadIndexEntries(const std::filesystem::path& path) {
  std::ifstream input{path, std::ios::binary};
  if (!input) {
    throw std::runtime_error{std::format("index file not found: {}", path.string())};
  }

  std::array<char, 8> magic{};
  uint64_t count = 0;
  input.read(magic.data(), magic.size());
  input.read(reinterpret_cast<char*>(&count), sizeof(count));
  if (!input || magic != kIntSortedIndexMagic) {
    throw std::runtime_error{std::format("invalid int sorted index file: {}", path.string())};
  }

  std::vector<IntIndexEntry> entries(count);
  input.read(reinterpret_cast<char*>(entries.data()),
             static_cast<std::streamsize>(entries.size() * sizeof(IntIndexEntry)));
  if (!input) {
    throw std::runtime_error{std::format("truncated index file: {}", path.string())};
  }
  return entries;
}

void WriteIndexEntries(const std::filesystem::path& path, const std::vector<IntIndexEntry>& entries) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  if (!output) {
    throw std::runtime_error{std::format("cannot write index file: {}", path.string())};
  }

  uint64_t count = entries.size();
  output.write(kIntSortedIndexMagic.data(), kIntSortedIndexMagic.size());
  output.write(reinterpret_cast<const char*>(&count), sizeof(count));
  output.write(reinterpret_cast<const char*>(entries.data()),
               static_cast<std::streamsize>(entries.size() * sizeof(IntIndexEntry)));
}

void BuildIntSortedIndex(const std::filesystem::path& csv_path,
                         const std::filesystem::path& index_path,
                         const std::string& output_table_name,
                         const std::string& column) {
  std::ifstream input{csv_path};
  if (!input) {
    throw std::runtime_error{std::format("CSV file not found: {}", csv_path.string())};
  }

  auto attrs = ReadAttributes(input, output_table_name);
  auto it = std::find_if(attrs.begin(), attrs.end(), [&](const AttributeInfo& attr) {
    return attr.name == column;
  });
  if (it == attrs.end()) {
    throw std::runtime_error{std::format("indexed column not found: {}", column)};
  }
  if (it->type != Type::kInt) {
    throw std::runtime_error{std::format("only int indexes are supported: {}", column)};
  }
  size_t column_index = static_cast<size_t>(it - attrs.begin());

  std::vector<IntIndexEntry> entries;
  std::string line;
  while (input) {
    auto row_offset = input.tellg();
    if (!std::getline(input, line)) {
      break;
    }
    auto tuple = ParseTuple(line, attrs);
    const auto& key = tuple[column_index];
    if (!key.is_null) {
      entries.push_back(IntIndexEntry{
          .key = key.value.int_value,
          .row_offset = static_cast<uint64_t>(row_offset),
      });
    }
  }

  std::ranges::sort(entries, [](const IntIndexEntry& lhs, const IntIndexEntry& rhs) {
    return std::tie(lhs.key, lhs.row_offset) < std::tie(rhs.key, rhs.row_offset);
  });
  WriteIndexEntries(index_path, entries);
}

std::pair<std::vector<IntIndexEntry>::const_iterator, std::vector<IntIndexEntry>::const_iterator>
FindIndexRange(const std::vector<IntIndexEntry>& entries, const SeekCondition& condition) {
  auto lower = [&](int64_t key) {
    return std::ranges::lower_bound(entries, key, {}, &IntIndexEntry::key);
  };
  auto upper = [&](int64_t key) {
    return std::ranges::upper_bound(entries, key, {}, &IntIndexEntry::key);
  };

  switch (condition.op) {
    case BinaryOp::kEq:
      return {lower(condition.key), upper(condition.key)};
    case BinaryOp::kLt:
      return {entries.begin(), lower(condition.key)};
    case BinaryOp::kLe:
      return {entries.begin(), upper(condition.key)};
    case BinaryOp::kGt:
      return {upper(condition.key), entries.end()};
    case BinaryOp::kGe:
      return {lower(condition.key), entries.end()};
    default:
      throw std::runtime_error{"unsupported index seek operator"};
  }
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
  auto attributes = ReadAttributes(input, output_table_name);

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

boost::asio::awaitable<Result<>> CsvDirIndexedScanner::operator()(
    const std::string& table_name, const std::string& output_table_name,
    const Expression& predicate,
    AttributesInfoChannel& attrs_chan,
    TuplesChannel& tuples_chan) const {
#ifdef DEBUG
  std::clog << "Executing index seek\n";
#endif
  auto condition = ExtractSeekCondition(predicate, table_name, output_table_name);
  if (!condition) {
    throw std::runtime_error{"IndexSeek predicate must be a comparison between one indexed int column and an int constant"};
  }

  std::filesystem::path data_dir{dir};
  auto meta = FindIndexMeta(data_dir, table_name, condition->column);
  if (!meta) {
    throw std::runtime_error{std::format("no sorted int index declared for {}.{}", table_name, condition->column)};
  }

  auto csv_path = data_dir / std::format("{}.csv", table_name);
  auto index_path = data_dir / meta->file;
  if (!std::filesystem::exists(index_path)) {
    BuildIntSortedIndex(csv_path, index_path, output_table_name, condition->column);
  }
  auto entries = ReadIndexEntries(index_path);

  std::ifstream input{csv_path};
  if (!input) {
    throw std::runtime_error{std::format("CSV file not found: {}", csv_path.string())};
  }
  auto attributes = ReadAttributes(input, output_table_name);
  auto filter = InterpretedExpressionExecutor{co_await boost::asio::this_coro::executor};
  auto residual = co_await filter.GetExpressionExecutor(predicate, attributes);

  co_await attrs_chan.async_send(boost::system::error_code{}, attributes,
                                 boost::asio::use_awaitable);
  attrs_chan.close();

  auto [begin, end] = FindIndexRange(entries, *condition);
  Tuples buf;
  buf.reserve(kBufSize);
  std::string line;
  for (auto it = begin; it != end; ++it) {
    input.clear();
    input.seekg(static_cast<std::streamoff>(it->row_offset));
    if (!std::getline(input, line)) {
      throw std::runtime_error{"failed to read indexed CSV row"};
    }
    auto tuple = ParseTuple(line, attributes);
    if (!ApplyFilter(tuple, attributes, residual)) {
      continue;
    }
    buf.emplace_back(std::move(tuple));
    if (buf.size() == kBufSize) {
      co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                      boost::asio::use_awaitable);
      buf.clear();
    }
  }

  if (!buf.empty()) {
    co_await tuples_chan.async_send(boost::system::error_code{}, std::move(buf),
                                    boost::asio::use_awaitable);
  }
  tuples_chan.close();

  co_return Ok();
}

}  // namespace stewkk::sql
