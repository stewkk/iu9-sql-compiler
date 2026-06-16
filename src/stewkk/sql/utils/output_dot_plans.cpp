#include <stewkk/sql/utils/output_dot_plans.hpp>

#include <filesystem>
#include <chrono>
#include <fstream>

#include <stewkk/sql/logic/executor/plan_serializer.hpp>

namespace stewkk::sql {

void OutputDot(const PhysicalPlanNode& optimizer_plan, const std::optional<PhysicalPlanNode> other_plan) {
  std::filesystem::create_directories(".plans");
  auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
  {
    std::ofstream dot_file{std::format(".plans/{}.dot", ts)};
    dot_file << SerializeDot(optimizer_plan);
  }
  if (other_plan) {
    std::ofstream dot_file{std::format(".plans/{}.ref.dot", ts)};
    dot_file << SerializeDot(*other_plan);
  }
}

}  // namespace stewkk::sql
