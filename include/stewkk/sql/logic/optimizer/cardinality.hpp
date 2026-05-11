#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include <stewkk/sql/utils/not_null.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

class CardinalityEstimates {
public:
  CardinalityEstimates(std::unordered_map<std::string, int64_t> table_sizes = {});

  int64_t GetCardinality(utils::NotNull<Group*> group);

private:
  int64_t GetCardinality(const LogicalOperator& op);

  std::unordered_map<std::string, int64_t> table_sizes_;
  std::unordered_map<Group*, int64_t> cache_;
};

}  // namespace stewkk::sql
