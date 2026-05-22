#include <stewkk/sql/logic/optimizer/winner_key.hpp>

#include <functional>

#include <boost/container_hash/hash.hpp>

namespace std {

size_t hash<stewkk::sql::WinnerKey>::operator()(const stewkk::sql::WinnerKey& k) const noexcept {
  size_t h = std::hash<stewkk::sql::Group*>{}(k.group);
  boost::hash_combine(h, std::hash<stewkk::sql::PropertySet>{}(k.required));
  return h;
}

}  // namespace std
