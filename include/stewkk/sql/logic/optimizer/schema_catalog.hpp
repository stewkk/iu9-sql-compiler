#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

using Schema = std::vector<Attribute>;

struct IndexInfo {
  std::string table;
  std::string column;
  std::string type;
  std::string file;
};

struct UniqueKeyInfo {
  std::string table;
  std::string column;
};

struct ForeignKeyInfo {
  std::string from_table;
  std::string from_column;
  std::string to_table;
  std::string to_column;
};

class IndexCatalog {
public:
  IndexCatalog(std::vector<IndexInfo> indexes = {});

  bool HasSortedIndex(const std::string& table, const std::string& column) const;

private:
  std::vector<IndexInfo> indexes_;
};

class ConstraintCatalog {
public:
  ConstraintCatalog(std::vector<UniqueKeyInfo> unique_keys = {},
                    std::vector<ForeignKeyInfo> foreign_keys = {});

  bool IsUnique(const Attribute& attr) const;
  bool HasForeignKey(const Attribute& from, const Attribute& to) const;

private:
  std::vector<UniqueKeyInfo> unique_keys_;
  std::vector<ForeignKeyInfo> foreign_keys_;
};

class SchemaCatalog {
public:
  SchemaCatalog(std::unordered_map<std::string, Schema> tables = {});

  Schema GetSchema(utils::NotNull<Group*> group);
  std::int64_t GetWidth(utils::NotNull<Group*> group);
  std::optional<Attribute> ResolveBaseAttribute(const Attribute& attr,
                                                utils::NotNull<Group*> group);

private:
  Schema Derive(const LogicalOperator& op);

  std::unordered_map<std::string, Schema> tables_;
  std::unordered_map<Group*, Schema> cache_;
};

SchemaCatalog LoadSchemaFromCsvDir(const std::filesystem::path& dir);

IndexCatalog LoadIndexCatalogFromCsvDir(const std::filesystem::path& dir);

ConstraintCatalog LoadConstraintCatalogFromCsvDir(const std::filesystem::path& dir);

std::unordered_map<std::string, std::int64_t> LoadTableSizesFromCsvDir(
    const std::filesystem::path& dir);

}  // namespace stewkk::sql
