#pragma once

#include <string>
#include <string_view>

#include <stewkk/sql/logic/executor/plan.hpp>

namespace stewkk::sql {

std::string Serialize(const PhysicalPlanNode& node);
PhysicalPlanNode Deserialize(std::string_view text);

std::string SerializeDot(const PhysicalPlanNode& node);

} // namespace stewkk::sql
