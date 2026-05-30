#pragma once

#include <istream>
#include <string>

#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/group.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>
#include <stewkk/sql/utils/not_null.hpp>

namespace stewkk::sql {

// FIXME: use std::expected
struct MatchResult {
    bool reachable;
    std::string mismatch;
};

MatchResult IsReachable(utils::NotNull<Group*> root, const PhysicalPlanNode& target);

// Runs an exhaustive search over `sql` and reports whether `target` is among
// the physical alternatives the optimizer enumerates. The query's ORDER BY (if
// any) is propagated as a required sort property so the search also generates
// the Sort enforcers needed to reach an ordered plan; `schema` lets those
// enforcers validate that the sort keys exist on the group they sit above.
MatchResult IsPlanReachable(std::istream& sql, const PhysicalPlanNode& target,
                             CardinalityEstimates cardinality = {},
                             SchemaCatalog schema = {});

}  // namespace stewkk::sql
