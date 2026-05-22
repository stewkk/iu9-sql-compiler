#pragma once

#include <cstddef>
#include <optional>

#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {

struct PropertySet {
  std::optional<SortOrder> sort;

  PropertySet() = default;
  explicit PropertySet(std::optional<SortOrder> sort);

  bool Satisfies(const PropertySet& required) const;

  static PropertySet Any();

  bool operator==(const PropertySet&) const = default;
};

}  // namespace stewkk::sql

namespace std {

template <>
struct hash<stewkk::sql::PropertySet> {
  size_t operator()(const stewkk::sql::PropertySet& ps) const noexcept;
};

}  // namespace std
