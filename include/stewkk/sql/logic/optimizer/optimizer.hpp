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
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>
#include <stewkk/sql/logic/optimizer/winner_key.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>

namespace stewkk::sql {

std::vector<utils::NotNull<Group*>> GetChildren(utils::NotNull<LogicalExpr*> expr);
std::vector<utils::NotNull<Group*>> GetChildren(utils::NotNull<PhysicalExpr*> expr);

template<size_t NTransformation, size_t NImplementation>
class Optimizer {
public:
  Optimizer(const Operator& expr, Rules<NTransformation, NImplementation>&& rules,
            CardinalityEstimates cardinality = {}, SchemaCatalog schema = {});

  PhysicalPlanNode Optimize(PropertySet required = PropertySet::Any());

  // Runs exhaustive search, populating all physical_exprs_ in every reachable
  // group.
  void OptimizeExhaustive();

  utils::NotNull<Group*> GetRootGroup() const;

private:
  using Limit = std::optional<std::int64_t>;
  void RunSearch(PropertySet required, Limit limit);

  bool IsExplored(utils::NotNull<Group*> group) const;
  void SetExplored(utils::NotNull<Group*> group);

  void OptimizeInputs(utils::NotNull<PhysicalExpr*> expr, PropertySet required, std::vector<PropertySet> child_delivered, int64_t accum, Limit limit, size_t child_index = 0);

  // Property-independent admissible lower bound on the cheapest physical plan
  // for this group: min over logical alternatives of (best-case local cost +
  // sum of children's lower bounds). Cached; safe to memoize once Phase 1
  // exploration has populated the group's logical alternatives.
  std::int64_t LowerBoundCost(utils::NotNull<Group*> group);

  void ApplyRule(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit);

  void TryRules(utils::NotNull<LogicalExpr*> expr, Limit limit);
  void ExploreExpression(utils::NotNull<LogicalExpr*> expr, Limit limit);
  void ExploreGroup(utils::NotNull<Group*> group, Limit limit);
  void OptimizeGroup(utils::NotNull<Group*> group, PropertySet required = PropertySet::Any(), Limit limit = std::nullopt);

  PhysicalPlanNode BuildOptimalPlan(Group* group, PropertySet required = PropertySet::Any());

  Memo memo_;
  RulesApplier<NTransformation, NImplementation> rules_applier_;
  std::stack<std::function<void()>> tasks_;
  std::unordered_set<Group*> explored_groups_;
  std::unordered_set<LogicalExpr*> explored_exprs_;
  utils::NotNull<LogicalExpr*> root_;
  CardinalityEstimates cardinality_;
  SchemaCatalog schema_;
  std::unordered_map<PhysicalExpr*, int64_t> local_cost_;
  std::unordered_map<WinnerKey, int64_t> best_cost_;
  std::unordered_map<WinnerKey, PhysicalExpr*> best_plan_;
  std::unordered_map<WinnerKey, PropertySet> best_delivered_;
  std::unordered_set<WinnerKey> enforcers_added_;
  std::unordered_map<Group*, std::vector<LogicalExpr*>> group_parents_;
  std::unordered_map<Group*, std::int64_t> lower_bounds_;
};

}  // namespace stewkk::sql
