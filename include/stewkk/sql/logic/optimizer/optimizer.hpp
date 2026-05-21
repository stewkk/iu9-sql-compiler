#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <stewkk/sql/logic/optimizer/cardinality.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rules_applier.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>

namespace stewkk::sql {

std::vector<utils::NotNull<Group*>> GetChildren(utils::NotNull<LogicalExpr*> expr);
std::vector<utils::NotNull<Group*>> GetChildren(utils::NotNull<PhysicalExpr*> expr);

template<size_t NTransformation, size_t NImplementation>
class Optimizer {
public:
  Optimizer(const Operator& expr, Rules<NTransformation, NImplementation>&& rules,
            CardinalityEstimates cardinality = {});

  PhysicalPlanNode Optimize();

  // Runs exhaustive search (no cost limit), populating all physical_exprs_ in
  // every reachable group. Invariant: must not be called with a top-level limit.
  // Use GetRootGroup() afterward to drive IsReachable().
  void OptimizeExhaustive();

  utils::NotNull<Group*> GetRootGroup() const;

private:
  void RunSearch();
  using Limit = std::optional<std::int64_t>;

  bool IsExplored(utils::NotNull<Group*> group) const;
  void SetExplored(utils::NotNull<Group*> group);

  void OptimizeInputs(utils::NotNull<PhysicalExpr*> expr, Limit limit, size_t child_index = 0);

  void ApplyRule(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit);
  void ApplyRule(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit);

  void OptimizeExpression(utils::NotNull<LogicalExpr*> expr, Limit limit);
  void ExploreExpression(utils::NotNull<LogicalExpr*> expr, Limit limit);
  void ExploreGroup(utils::NotNull<Group*> group, Limit limit);
  void OptimizeGroup(utils::NotNull<Group*> group, Limit limit = std::nullopt);

  PhysicalPlanNode BuildOptimalPlan(Group* group);

  Memo memo_;
  RulesApplier<NTransformation, NImplementation> rules_applier_;
  std::stack<std::function<void()>> tasks_;
  std::unordered_set<Group*> explored_groups_;
  utils::NotNull<LogicalExpr*> root_;
  CardinalityEstimates cardinality_;
  std::unordered_map<PhysicalExpr*, int64_t> local_cost_;
  std::unordered_map<PhysicalExpr*, int64_t> accum_child_cost_;
  std::unordered_map<Group*, int64_t> best_cost_;
  std::unordered_map<Group*, PhysicalExpr*> best_plan_;
};

}  // namespace stewkk::sql
