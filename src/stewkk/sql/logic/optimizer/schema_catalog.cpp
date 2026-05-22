#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

SchemaCatalog::SchemaCatalog(std::unordered_map<std::string, Schema> tables)
    : tables_(std::move(tables)) {}

std::optional<Schema> SchemaCatalog::GetSchema(utils::NotNull<Group*> group) {
  if (auto it = cache_.find(group.get()); it != cache_.end()) {
    return it->second;
  }
  auto schema = Derive(group->GetLogicalExprs().front()->root_operator);
  cache_[group.get()] = schema;
  return schema;
}

// TODO: refactor to remove duplicate logic: both executor and optimizer derive
// attributes
std::optional<Schema> SchemaCatalog::Derive(const LogicalOperator& op) {
  return std::visit(utils::Overloaded{
      [this](const logical::Table& t) -> std::optional<Schema> {
          auto it = tables_.find(t.name);
          if (it == tables_.end()) return std::nullopt;
          return it->second;
      },
      [this](const logical::Filter& f) -> std::optional<Schema> {
          return GetSchema(f.source);
      },
      [](const logical::Projection& p) -> std::optional<Schema> {
          // Schema after projection is exactly the Attributes named in its
          // expression list. Computed (non-Attribute) expressions produce
          // columns that can't be referenced by (table, name), so they are
          // dropped from the schema — sorting on them is impossible anyway.
          Schema out;
          for (const auto& expr : p.expressions) {
              if (const auto* a = std::get_if<Attribute>(&expr)) {
                  out.push_back(*a);
              }
          }
          return out;
      },
      [this](const logical::CrossJoin& j) -> std::optional<Schema> {
          auto l = GetSchema(j.lhs);
          auto r = GetSchema(j.rhs);
          if (!l || !r) return std::nullopt;
          l->insert(l->end(), r->begin(), r->end());
          return l;
      },
      [this](const logical::Join& j) -> std::optional<Schema> {
          auto l = GetSchema(j.lhs);
          auto r = GetSchema(j.rhs);
          if (!l || !r) return std::nullopt;
          l->insert(l->end(), r->begin(), r->end());
          return l;
      },
  }, op);
}

}  // namespace stewkk::sql
