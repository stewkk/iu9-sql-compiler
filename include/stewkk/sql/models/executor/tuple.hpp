#pragma once

#include <vector>
#include <string>
#include <variant>

namespace stewkk::sql {

struct NullValue {
    auto operator<=>(const NullValue& other) const = default;
};

enum class Type {
    kInt,
    kBool,
};

struct AttributeInfo {
    std::string table;
    std::string name;
    Type type;

    auto operator<=>(const AttributeInfo& other) const = default;
};

enum class Trilean {
   kTrue,
   kFalse,
   kUnknown,
};

union NonNullValue {
    int64_t int_value;
    Trilean trilean_value;
};

std::string ToString(Trilean v);

NonNullValue GetTrileanValue(Trilean v);

using Value = std::variant<NonNullValue, NullValue>;
using Tuple = std::vector<Value>;
using Tuples = std::vector<Tuple>;
using AttributesInfo = std::vector<AttributeInfo>;

struct Relation {
    AttributesInfo attributes;
    Tuples tuples;
};

std::string ToString(const Relation& relation);

}  // namespace stewkk::sql
