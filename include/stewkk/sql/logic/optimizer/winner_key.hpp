#pragma once

#include <cstddef>
#include <functional>

#include <stewkk/sql/logic/optimizer/group.hpp>
#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>

namespace stewkk::sql {

struct WinnerKey {
  Group* group;
  PropertySet required;
  bool operator==(const WinnerKey&) const = default;
};

}  // namespace stewkk::sql

namespace std {

template <>
struct hash<stewkk::sql::WinnerKey> {
  size_t operator()(const stewkk::sql::WinnerKey& k) const noexcept;
};

}  // namespace std
