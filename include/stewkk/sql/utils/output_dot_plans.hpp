#pragma once

#include <stewkk/sql/logic/executor/plan.hpp>

namespace stewkk::sql {

void OutputDot(const PhysicalPlanNode& optimizer_plan, const std::optional<PhysicalPlanNode> other_plan);

} // namespace stewkk::sql
