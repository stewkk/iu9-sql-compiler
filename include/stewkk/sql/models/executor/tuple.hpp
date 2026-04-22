#pragma once

#include <vector>
#include <string>

namespace stewkk::sql {

enum class Type {
    kInt,
    kBool,
};

std::string ToString(Type type);

struct AttributeInfo {
    std::string table;
    std::string name;
    Type type;

    auto operator<=>(const AttributeInfo& other) const = default;
};

// NOTE: union leaves possibility to add other data types later
union NonNullValue {
    int64_t int_value;
    bool bool_value;
};

std::string ToString(bool v);

// NOTE: bare struct for compatibility with llvm
struct Value {
    bool is_null;
    NonNullValue value;

    bool operator==(const Value& other) const;
};

std::string ToString(Value v, const AttributeInfo& attr);

using Tuple = std::vector<Value>;
using Tuples = std::vector<Tuple>;
using AttributesInfo = std::vector<AttributeInfo>;

struct Relation {
    AttributesInfo attributes;
    Tuples tuples;
};

std::string ToString(const Relation& relation);

}  // namespace stewkk::sql
