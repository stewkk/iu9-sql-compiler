#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

using Schema = std::vector<Attribute>;

class SchemaCatalog {
public:
  SchemaCatalog(std::unordered_map<std::string, Schema> tables = {});

  // Returns nullopt when any table in the subtree is unknown to the catalog.
  std::optional<Schema> GetSchema(utils::NotNull<Group*> group);

private:
  std::optional<Schema> Derive(const LogicalOperator& op);

  std::unordered_map<std::string, Schema> tables_;
  std::unordered_map<Group*, std::optional<Schema>> cache_;
};

}  // namespace stewkk::sql
