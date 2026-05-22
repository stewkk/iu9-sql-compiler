#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>

#include <boost/container_hash/hash.hpp>

namespace stewkk::sql {

PropertySet::PropertySet(std::optional<SortOrder> s) : sort(std::move(s)) {
  // derive implied properties from sort here as new property types are added
}

PropertySet PropertySet::Any() { return {}; }

bool PropertySet::Satisfies(const PropertySet& required) const {
  if (required.sort) {
    if (!sort.has_value() || !sort->Satisfies(required.sort.value())) {
      return false;
    }
  }
  return true;
}

}  // namespace stewkk::sql

namespace std {

size_t hash<stewkk::sql::PropertySet>::operator()(const stewkk::sql::PropertySet& ps) const noexcept {
  size_t h = 0;
  if (ps.sort)
    boost::hash_combine(h, hash<stewkk::sql::SortOrder>{}(*ps.sort));
  return h;
}

}  // namespace std
