#pragma once

#include <optional>

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/logic/optimizer/enforcer.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>
#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>

namespace stewkk::sql {

class SortEnforcer final : public Enforcer {
public:
  std::optional<PhysicalOperator> TryBuild(
      utils::NotNull<Group*> group,
      const PropertySet& required,
      SchemaCatalog& schema) const override;
};

}  // namespace stewkk::sql
