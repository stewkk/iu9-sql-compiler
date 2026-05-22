#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace stewkk::sql {

enum class Direction { kAsc, kDesc };

struct SortKey {
  std::string column;
  Direction dir;

  bool operator==(const SortKey&) const = default;
};

struct SortOrder {
  std::vector<SortKey> keys;

  bool Satisfies(const SortOrder& required) const;

  bool operator==(const SortOrder&) const = default;
};

}  // namespace stewkk::sql

namespace std {

template <>
struct hash<stewkk::sql::SortKey> {
  size_t operator()(const stewkk::sql::SortKey& k) const noexcept;
};

template <>
struct hash<stewkk::sql::SortOrder> {
  size_t operator()(const stewkk::sql::SortOrder& o) const noexcept;
};

}  // namespace std
