#pragma once

#include <istream>
#include <variant>

namespace stewkk::sql {

struct Table {
    std::string name;

    auto operator<=>(const Table& other) const = default;
};
using Operator = std::variant<Table>;

Operator GetAST(std::istream& in);

}  // namespace stewkk::sql
