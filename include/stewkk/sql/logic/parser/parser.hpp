#pragma once

#include <istream>
#include <memory>
#include <variant>
#include <vector>

namespace stewkk::sql {

struct Table;
struct Projection;

using Operator = std::variant<Table, Projection>;

struct Table {
    std::string name;

    auto operator<=>(const Table& other) const = default;
};

struct Projection {
    std::vector<std::string> attributes;
    std::shared_ptr<Operator> source;

    bool operator==(const Projection& other) const;
};

Operator GetAST(std::istream& in);

}  // namespace stewkk::sql
