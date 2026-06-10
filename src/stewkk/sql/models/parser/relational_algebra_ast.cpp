#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

std::string_view VisibleName(const Table& table) {
  return table.alias ? std::string_view{*table.alias} : std::string_view{table.name};
}

bool Projection::operator==(const Projection& other) const {
  auto normalize = [](const std::vector<std::optional<std::string>>& aliases) {
    auto result = aliases;
    while (!result.empty() && !result.back()) {
      result.pop_back();
    }
    return result;
  };
  return expressions == other.expressions && *source == *other.source
         && normalize(aliases) == normalize(other.aliases);
}

bool Filter::operator==(const Filter& other) const {
  return expr == other.expr && *source == *other.source;
}

bool Aggregation::operator==(const Aggregation& other) const {
  return group_by == other.group_by && aggregates == other.aggregates && *source == *other.source;
}

bool CrossJoin::operator==(const CrossJoin& other) const {
  return *lhs == *other.lhs && *rhs == *other.rhs;
}

bool Join::operator==(const Join& other) const {
  return type == other.type && qual == other.qual && *lhs == *other.lhs && *rhs == *other.rhs;
}

} // namespace stewkk::sql
