#include <stewkk/sql/logic/optimizer/sort_enforcer.hpp>

#include <stewkk/sql/logic/optimizer/physical_expr.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>

namespace stewkk::sql {

std::optional<PhysicalOperator> SortEnforcer::TryBuild(
    utils::NotNull<Group*> group,
    const PropertySet& required,
    SchemaCatalog& schema) const {
  const auto* req = required.Get<SortProperty>();
  if (!req) return std::nullopt;
  auto sch = schema.GetSchema(group);
  for (const auto& sk : req->order.keys) {
    bool found = false;
    for (const auto& a : sch) {
      if (a.name == sk.column && (sk.table.empty() || a.table == sk.table)) {
        found = true;
        break;
      }
    }
    if (!found) return std::nullopt;
  }
  return PhysicalOperator{physical::Sort{group, req->order}};
}

}  // namespace stewkk::sql
