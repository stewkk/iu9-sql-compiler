#include <stewkk/sql/models/executor/tuple.hpp>

#include <sstream>
#include <ranges>

namespace stewkk::sql {

std::string ToString(Type type) {
  switch (type) {
    case Type::kInt:
      return "int";
    case Type::kBool:
      return "bool";
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

}  // namespace stewkk::sql
