#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>

#include <algorithm>
#include <fstream>
#include <format>
#include <ranges>
#include <regex>
#include <sstream>
#include <utility>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

IndexCatalog::IndexCatalog(std::vector<IndexInfo> indexes)
    : indexes_(std::move(indexes)) {}

bool IndexCatalog::HasSortedIndex(const std::string& table, const std::string& column) const {
  return std::ranges::any_of(indexes_, [&](const IndexInfo& index) {
    return index.table == table && index.column == column && index.type == "sorted";
  });
}

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

std::int64_t SchemaCatalog::GetWidth(utils::NotNull<Group*> group) {
  auto schema = GetSchema(group);
  return schema ? std::max<std::int64_t>(1, schema->size()) : 1;
}

// TODO: refactor to remove duplicate logic: both executor and optimizer derive
// attributes
std::optional<Schema> SchemaCatalog::Derive(const LogicalOperator& op) {
  return std::visit(utils::Overloaded{
      [this](const logical::Table& t) -> std::optional<Schema> {
          auto it = tables_.find(t.name);
          if (it == tables_.end()) return std::nullopt;
          auto schema = it->second;
          for (auto& attr : schema) {
              attr.table = std::string{VisibleName(t)};
          }
          return schema;
      },
      [this](const logical::Filter& f) -> std::optional<Schema> {
          return GetSchema(f.source);
      },
      [](const logical::Projection& p) -> std::optional<Schema> {
          Schema out;
          for (size_t i = 0; i < p.expressions.size(); ++i) {
              if (i < p.aliases.size() && p.aliases[i]) {
                  out.push_back(Attribute{"", *p.aliases[i]});
              } else if (const auto* a = std::get_if<Attribute>(&p.expressions[i])) {
                  out.push_back(*a);
              }
          }
          return out;
      },
      [this](const logical::Aggregation& a) -> std::optional<Schema> {
          auto input = GetSchema(a.source);
          if (!input) return std::nullopt;
          Schema out;
          for (const auto& expr : a.group_by) {
              if (const auto* attr = std::get_if<Attribute>(&expr)) {
                  out.push_back(*attr);
              }
          }
          for (size_t i = 0; i < a.aggregates.size(); ++i) {
              out.push_back(Attribute{"", std::format("__agg{}", i)});
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

SchemaCatalog LoadSchemaFromCsvDir(const std::filesystem::path& dir) {
  std::unordered_map<std::string, Schema> tables;
  if (!std::filesystem::is_directory(dir)) {
    return SchemaCatalog{};
  }

  static const std::regex kBench{R"(_\d+$)"};
  for (const auto& entry : std::filesystem::directory_iterator{dir}) {
    if (entry.path().extension() != ".csv") continue;
    auto stem = entry.path().stem().string();
    if (std::regex_search(stem, kBench)) continue;

    std::ifstream in{entry.path()};
    std::string header;
    if (!std::getline(in, header)) continue;

    Schema schema;
    for (const auto& part : header | std::views::split(',')) {
      std::string token{part.begin(), part.end()};
      auto colon = token.find(':');
      if (colon == std::string::npos) continue;
      schema.push_back(Attribute{stem, token.substr(0, colon)});
    }
    tables.emplace(std::move(stem), std::move(schema));
  }
  return SchemaCatalog{std::move(tables)};
}

IndexCatalog LoadIndexCatalogFromCsvDir(const std::filesystem::path& dir) {
  std::vector<IndexInfo> indexes;
  std::ifstream input{dir / "indexes.meta"};
  if (!input) {
    return IndexCatalog{};
  }

  std::string line;
  while (std::getline(input, line)) {
    auto first = line.find_first_not_of(" \t\r\n");
    if (first == std::string::npos || line[first] == '#') {
      continue;
    }

    std::istringstream fields{line};
    IndexInfo index;
    if (fields >> index.table >> index.column >> index.type >> index.file) {
      indexes.push_back(std::move(index));
    }
  }
  return IndexCatalog{std::move(indexes)};
}

std::unordered_map<std::string, std::int64_t> LoadTableSizesFromCsvDir(
    const std::filesystem::path& dir) {
  std::unordered_map<std::string, std::int64_t> sizes;
  if (!std::filesystem::is_directory(dir)) {
    return sizes;
  }

  static const std::regex kBench{R"(_\d+$)"};
  for (const auto& entry : std::filesystem::directory_iterator{dir}) {
    if (entry.path().extension() != ".csv") continue;
    auto stem = entry.path().stem().string();
    if (std::regex_search(stem, kBench)) continue;

    std::ifstream in{entry.path()};
    std::string line;
    if (!std::getline(in, line)) continue;  // skip header

    std::int64_t rows = 0;
    while (std::getline(in, line)) {
      if (!line.empty()) ++rows;
    }
    sizes.emplace(std::move(stem), rows);
  }
  return sizes;
}

}  // namespace stewkk::sql
