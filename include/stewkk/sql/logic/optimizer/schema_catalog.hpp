#pragma once

#include <cstdint>
#include <filesystem>
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

class IndexCatalog {
public:
  IndexCatalog(std::vector<IndexInfo> indexes = {});

  bool HasSortedIndex(const std::string& table, const std::string& column) const;

private:
  std::vector<IndexInfo> indexes_;
};

class SchemaCatalog {
public:
  SchemaCatalog(std::unordered_map<std::string, Schema> tables = {});

  Schema GetSchema(utils::NotNull<Group*> group);
  std::int64_t GetWidth(utils::NotNull<Group*> group);

private:
  Schema Derive(const LogicalOperator& op);

  std::unordered_map<std::string, Schema> tables_;
  std::unordered_map<Group*, Schema> cache_;
};

SchemaCatalog LoadSchemaFromCsvDir(const std::filesystem::path& dir);

IndexCatalog LoadIndexCatalogFromCsvDir(const std::filesystem::path& dir);

std::unordered_map<std::string, std::int64_t> LoadTableSizesFromCsvDir(
    const std::filesystem::path& dir);

}  // namespace stewkk::sql
