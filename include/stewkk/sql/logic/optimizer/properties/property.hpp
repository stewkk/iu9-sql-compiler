#pragma once

#include <cstddef>
#include <memory>

namespace stewkk::sql {

class Property {
public:
  virtual ~Property() = default;

  virtual std::unique_ptr<Property> Clone() const = 0;
  virtual bool Satisfies(const Property& required) const = 0;
  virtual bool Equals(const Property& other) const = 0;
  virtual std::size_t Hash() const = 0;
};

template <class Derived>
class PropertyBase : public Property {
public:
  bool Satisfies(const Property& other) const final {
    return static_cast<const Derived*>(this)->SatisfiesTyped(
        static_cast<const Derived&>(other));
  }
  bool Equals(const Property& other) const final {
    return static_cast<const Derived*>(this)->EqualsTyped(
        static_cast<const Derived&>(other));
  }
};

}  // namespace stewkk::sql
