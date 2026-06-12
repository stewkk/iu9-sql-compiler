#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>

#include <algorithm>
#include <typeindex>
#include <typeinfo>

#include <boost/container_hash/hash.hpp>

namespace stewkk::sql {

PropertySet::PropertySet(const PropertySet& other) {
  props_.reserve(other.props_.size());
  for (const auto& p : other.props_) props_.push_back(p->Clone());
}

PropertySet& PropertySet::operator=(const PropertySet& other) {
  if (this == &other) return *this;
  std::vector<std::unique_ptr<Property>> copy;
  copy.reserve(other.props_.size());
  for (const auto& p : other.props_) copy.push_back(p->Clone());
  props_ = std::move(copy);
  return *this;
}

PropertySet PropertySet::Any() { return {}; }

const std::vector<std::unique_ptr<Property>>& PropertySet::Items() const noexcept { return props_; }

void PropertySet::Normalize() {
  std::ranges::sort(props_, {}, [](const std::unique_ptr<Property>& p) {
    const Property& prop = *p;
    return std::type_index(typeid(prop));
  });
}

bool PropertySet::Satisfies(const PropertySet& required) const {
  for (const auto& req : required.props_) {
    const Property& req_ref = *req;
    auto it = std::ranges::find_if(props_, [&](const std::unique_ptr<Property>& d) {
      const Property& d_ref = *d;
      return typeid(d_ref) == typeid(req_ref);
    });
    if (it == props_.end()) return false;
    if (!(*it)->Satisfies(*req)) return false;
  }
  return true;
}

bool PropertySet::operator==(const PropertySet& other) const {
  if (props_.size() != other.props_.size()) return false;
  for (std::size_t i = 0; i < props_.size(); ++i) {
    const Property& a = *props_[i];
    const Property& b = *other.props_[i];
    if (typeid(a) != typeid(b)) return false;
    if (!a.Equals(b)) return false;
  }
  return true;
}

}  // namespace stewkk::sql

namespace std {

size_t hash<stewkk::sql::PropertySet>::operator()(const stewkk::sql::PropertySet& ps) const noexcept {
  size_t h = 0;
  for (const auto& p : ps.Items()) {
    const stewkk::sql::Property& prop = *p;
    boost::hash_combine(h, std::type_index(typeid(prop)).hash_code());
    boost::hash_combine(h, prop.Hash());
  }
  return h;
}

}  // namespace std
