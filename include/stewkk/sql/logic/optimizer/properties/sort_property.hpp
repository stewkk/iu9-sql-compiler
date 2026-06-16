#pragma once

#include <cstddef>
#include <memory>

#include <stewkk/sql/logic/optimizer/properties/property.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {

class SortProperty final : public PropertyBase<SortProperty> {
public:
  SortOrder order;

  SortProperty() = default;
  explicit SortProperty(SortOrder o) : order(std::move(o)) {}

  std::unique_ptr<Property> Clone() const override;
  bool SatisfiesTyped(const SortProperty& required) const;
  bool EqualsTyped(const SortProperty& other) const;
  std::size_t Hash() const override;
};

}  // namespace stewkk::sql
