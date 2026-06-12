#include <stewkk/sql/models/executor/tuple.hpp>

#include <deque>
#include <format>
#include <mutex>
#include <sstream>
#include <ranges>
#include <stdexcept>
#include <unordered_map>

namespace stewkk::sql {

namespace {

std::mutex& StringPoolMutex() {
  static std::mutex mutex;
  return mutex;
}

std::deque<std::string>& StringPoolValues() {
  static std::deque<std::string> values;
  return values;
}

std::unordered_map<std::string, int64_t>& StringPoolIds() {
  static std::unordered_map<std::string, int64_t> ids;
  return ids;
}

} // namespace

std::string ToString(Type type) {
  switch (type) {
    case Type::kInt:
      return "int";
    case Type::kBool:
      return "bool";
    case Type::kString:
      return "string";
  }
  std::unreachable();
}

std::string ToString(bool v) {
    if (v) {
        return "TRUE    ";
    }
    return "FALSE   ";
}

std::string ToString(Value v, const AttributeInfo& attr) {
    if (v.is_null) {
        return "NULL    ";
    }
    if (attr.type == Type::kInt) {
      return std::format("{:<8}", v.value.int_value);
    }
    if (attr.type == Type::kString) {
      return std::format("{:<8}", GetInternedString(v.value.string_id));
    }
    return ToString(v.value.bool_value)+' ';
}

std::string ToString(const Relation& relation) {
    std::ostringstream s;
    for (const auto& tuple : relation.tuples) {
        for (const auto& [val, attr] : std::views::zip(tuple, relation.attributes)) {
            s << ToString(val, attr);
        }
        s << '\n';
    }

    return s.str();
}

bool Value::operator==(const Value& other) const {
    return (is_null && other.is_null) || (!is_null && !other.is_null && value.int_value == other.value.int_value);
}

int64_t InternString(std::string value) {
  std::lock_guard lock{StringPoolMutex()};
  auto& ids = StringPoolIds();
  if (auto it = ids.find(value); it != ids.end()) {
    return it->second;
  }
  auto& values = StringPoolValues();
  auto id = static_cast<int64_t>(values.size());
  values.push_back(std::move(value));
  ids.emplace(values.back(), id);
  return id;
}

const std::string& GetInternedString(int64_t id) {
  std::lock_guard lock{StringPoolMutex()};
  auto& values = StringPoolValues();
  if (id < 0 || static_cast<size_t>(id) >= values.size()) {
    throw std::out_of_range{"string id is not interned"};
  }
  return values[static_cast<size_t>(id)];
}

}  // namespace stewkk::sql
