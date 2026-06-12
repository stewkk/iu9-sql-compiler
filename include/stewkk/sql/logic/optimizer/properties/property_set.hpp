#pragma once

#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <typeinfo>
#include <vector>

#include <stewkk/sql/logic/optimizer/properties/property.hpp>

namespace stewkk::sql {

class PropertySet {
public:
  PropertySet() = default;
  PropertySet(const PropertySet& other);
  PropertySet(PropertySet&&) noexcept = default;
  PropertySet& operator=(const PropertySet& other);
  PropertySet& operator=(PropertySet&&) noexcept = default;
  ~PropertySet() = default;

  template <class... Ps>
    requires (sizeof...(Ps) > 0) &&
             (std::derived_from<std::decay_t<Ps>, Property> && ...)
  explicit PropertySet(Ps&&... ps) {
    props_.reserve(sizeof...(Ps));
    (props_.push_back(std::make_unique<std::decay_t<Ps>>(std::forward<Ps>(ps))), ...);
    Normalize();
  }

  static PropertySet Any();

  bool Satisfies(const PropertySet& required) const;

  template <class T>
  const T* Get() const noexcept {
    for (const auto& p : props_) {
      const Property& prop = *p;
      if (typeid(prop) == typeid(T)) return static_cast<const T*>(&prop);
    }
    return nullptr;
  }

  const std::vector<std::unique_ptr<Property>>& Items() const noexcept;

  bool operator==(const PropertySet& other) const;

private:
  void Normalize();

  std::vector<std::unique_ptr<Property>> props_;
};

}  // namespace stewkk::sql

namespace std {

template <>
struct hash<stewkk::sql::PropertySet> {
  size_t operator()(const stewkk::sql::PropertySet& ps) const noexcept;
};

}  // namespace std
