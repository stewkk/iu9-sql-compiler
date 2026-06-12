#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

#include <boost/container_hash/hash.hpp>

namespace stewkk::sql {

bool SortOrder::Satisfies(const SortOrder& required) const {
  if (required.keys.size() > keys.size()) {
    return false;
  }
  for (size_t i = 0; i < required.keys.size(); ++i) {
    if (keys[i] != required.keys[i]) {
      return false;
    }
  }
  return true;
}

}  // namespace stewkk::sql

namespace std {

size_t hash<stewkk::sql::SortKey>::operator()(const stewkk::sql::SortKey& k) const noexcept {
  size_t h = hash<string>{}(k.table);
  boost::hash_combine(h, hash<string>{}(k.column));
  boost::hash_combine(h, static_cast<int>(k.dir));
  return h;
}

size_t hash<stewkk::sql::SortOrder>::operator()(const stewkk::sql::SortOrder& o) const noexcept {
  size_t h = 0;
  for (const auto& key : o.keys) {
    boost::hash_combine(h, hash<stewkk::sql::SortKey>{}(key));
  }
  return h;
}

}  // namespace std
