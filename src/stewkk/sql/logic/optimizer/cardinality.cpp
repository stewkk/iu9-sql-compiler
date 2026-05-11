#include <stewkk/sql/logic/optimizer/cardinality.hpp>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

CardinalityEstimates::CardinalityEstimates(std::unordered_map<std::string, int64_t> table_sizes)
    : table_sizes_(std::move(table_sizes)) {}

int64_t CardinalityEstimates::GetCardinality(utils::NotNull<Group*> group) {
  if (auto it = cache_.find(group.get()); it != cache_.end()) {
    return it->second;
  }
  auto cardinality = GetCardinality(group->GetLogicalExprs().front()->root_operator);
  cache_[group.get()] = cardinality;
  return cardinality;
}

int64_t CardinalityEstimates::GetCardinality(const LogicalOperator& op) {
  return std::visit(utils::Overloaded{
      [this](const logical::Table& t) -> int64_t {
          if (auto it = table_sizes_.find(t.name); it != table_sizes_.end()) {
              return it->second;
          }
          return 10;
      },
      [this](const logical::Filter& f) -> int64_t {
          return GetCardinality(f.source);
      },
      [this](const logical::Projection& p) -> int64_t {
          return GetCardinality(p.source);
      },
      [this](const logical::CrossJoin& j) -> int64_t {
          return GetCardinality(j.lhs) * GetCardinality(j.rhs);
      },
      [this](const logical::Join& j) -> int64_t {
          return GetCardinality(j.lhs) * GetCardinality(j.rhs);
      },
  }, op);
}

}  // namespace stewkk::sql
