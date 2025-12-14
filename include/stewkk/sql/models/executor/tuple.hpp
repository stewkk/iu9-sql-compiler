#pragma once

#include <vector>
#include <string>
#include <variant>

namespace stewkk::sql {

struct NullValue{
    auto operator<=>(const NullValue& other) const = default;
};

struct AttributeValue {
    std::string table;
    std::string name;
    std::variant<NullValue, int64_t, bool> value;

    auto operator<=>(const AttributeValue& other) const = default;
};

using Tuple = std::vector<AttributeValue>;

}  // namespace stewkk::sql
