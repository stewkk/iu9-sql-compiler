#pragma once

#include <istream>
#include <string>

#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>
#include <stewkk/sql/utils/not_null.hpp>

namespace stewkk::sql {

// FIXME: use std::expected
struct MatchResult {
    bool reachable;
    std::string mismatch;
};

MatchResult IsReachable(utils::NotNull<Group*> root, const PhysicalPlanNode& target);

MatchResult IsPlanReachable(std::istream& sql, const PhysicalPlanNode& target,
                             CardinalityEstimates cardinality = {});

}  // namespace stewkk::sql
