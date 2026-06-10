#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>

#include <functional>

namespace stewkk::sql {

std::unique_ptr<Property> SortProperty::Clone() const {
  return std::make_unique<SortProperty>(*this);
}

bool SortProperty::SatisfiesTyped(const SortProperty& required) const {
  return order.Satisfies(required.order);
}

bool SortProperty::EqualsTyped(const SortProperty& other) const {
  return order == other.order;
}

std::size_t SortProperty::Hash() const {
  return std::hash<SortOrder>{}(order);
}

}  // namespace stewkk::sql
