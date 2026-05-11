#include <stewkk/sql/logic/optimizer/optimizer.hpp>

#include <stdexcept>

#include <stewkk/sql/utils/overloaded.hpp>
#include <stewkk/sql/utils/log.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>

namespace stewkk::sql {

std::vector<utils::NotNull<Group*>> GetChildren(utils::NotNull<LogicalExpr*> expr) {
  return std::visit(utils::Overloaded{
      [](const logical::Table&) -> std::vector<utils::NotNull<Group*>> {
          return {};
      },
      [](const logical::Filter& f) -> std::vector<utils::NotNull<Group*>> {
          return {f.source};
      },
      [](const logical::Projection& p) -> std::vector<utils::NotNull<Group*>> {
          return {p.source};
      },
      [](const logical::CrossJoin& j) -> std::vector<utils::NotNull<Group*>> {
          return {j.lhs, j.rhs};
      },
      [](const logical::Join& j) -> std::vector<utils::NotNull<Group*>> {
          return {j.lhs, j.rhs};
      },
  }, expr->root_operator);
}

std::vector<utils::NotNull<Group*>> GetChildren(utils::NotNull<PhysicalExpr*> expr) {
  return std::visit(utils::Overloaded{
      [](const physical::SeqScan&) -> std::vector<utils::NotNull<Group*>> {
          return {};
      },
      [](const physical::Filter& f) -> std::vector<utils::NotNull<Group*>> {
          return {f.source};
      },
      [](const physical::Projection& p) -> std::vector<utils::NotNull<Group*>> {
          return {p.source};
      },
      [](const physical::NestedLoopJoin& j) -> std::vector<utils::NotNull<Group*>> {
          return {j.lhs, j.rhs};
      },
      [](const physical::NestedLoopCrossJoin& j) -> std::vector<utils::NotNull<Group*>> {
          return {j.lhs, j.rhs};
      },
  }, expr->root_operator);
}

static int64_t CalcCost(utils::NotNull<PhysicalExpr*> expr, CardinalityEstimates& cardinality) {
  return std::visit(utils::Overloaded{
      [&](const physical::SeqScan&) -> int64_t {
          return cardinality.GetCardinality(expr->group);
      },
      [&](const physical::Filter&) -> int64_t {
          return cardinality.GetCardinality(expr->group);
      },
      [&](const physical::Projection&) -> int64_t {
          return cardinality.GetCardinality(expr->group);
      },
      [&](const physical::NestedLoopJoin& j) -> int64_t {
          auto p_l = (cardinality.GetCardinality(j.lhs) + kBufSize - 1) / kBufSize;
          auto p_r = (cardinality.GetCardinality(j.rhs) + kBufSize - 1) / kBufSize;
          return p_l * (1 + p_r);
      },
      [&](const physical::NestedLoopCrossJoin& j) -> int64_t {
          auto p_l = (cardinality.GetCardinality(j.lhs) + kBufSize - 1) / kBufSize;
          auto p_r = (cardinality.GetCardinality(j.rhs) + kBufSize - 1) / kBufSize;
          return p_l * (1 + p_r);
      },
  }, expr->root_operator);
}

template<size_t NTransformation, size_t NImplementation>
Optimizer<NTransformation, NImplementation>::Optimizer(
    const Operator& expr, Rules<NTransformation, NImplementation>&& rules,
    CardinalityEstimates cardinality)
    : memo_(), rules_applier_(std::move(rules)), root_(memo_.Populate(expr)),
      cardinality_(std::move(cardinality)) {}

template<size_t NTransformation, size_t NImplementation>
bool Optimizer<NTransformation, NImplementation>::IsExplored(utils::NotNull<Group*> group) const {
  return explored_groups_.contains(group.get());
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::SetExplored(utils::NotNull<Group*> group) {
  explored_groups_.insert(group);
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::OptimizeInputs(
    utils::NotNull<PhysicalExpr*> expr, Limit limit, size_t child_index) {
  auto children = GetChildren(expr);
  if (child_index >= children.size()) {
    int64_t total = local_cost_[expr.get()] + accum_child_cost_[expr.get()];
    auto* g = expr->group.get();
    if (!best_cost_.contains(g) || total < best_cost_.at(g)) {
      Log("New best plan for group {} with cost {}", g->GetId(), total);
      best_cost_[g] = total;
      best_plan_[g] = expr.get();
    }
    return;
  }

  auto child = children[child_index];

  tasks_.emplace([this, expr, child, child_index, limit]() {
    if (!best_cost_.contains(child.get())) return;
    auto cc = best_cost_.at(child.get());
    accum_child_cost_[expr.get()] += cc;
    Limit next = limit ? std::optional{*limit - local_cost_[expr.get()] - accum_child_cost_[expr.get()]} : std::nullopt;
    if (next && *next < 0) return;
    OptimizeInputs(expr, next, child_index + 1);
  });

  tasks_.emplace([this, child, limit]() {
    OptimizeGroup(child, limit);
  });
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::ApplyRule(
    TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit) {
  Log("Applying transformation rule {} to group {}", rule.value, expr->group->GetId());
  auto new_expr = rules_applier_.Apply(rule, expr, memo_);
  tasks_.emplace([this, new_expr, limit]() { ExploreExpression(new_expr, limit); });
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::ApplyRule(
    ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit) {
  Log("Applying implementation rule {} to group {}", rule.value, expr->group->GetId());
  auto new_expr = rules_applier_.Apply(rule, expr, memo_);
  auto lc = CalcCost(new_expr, cardinality_);
  Log("Local cost for group {} expression: {}", new_expr->group->GetId(), lc);
  local_cost_[new_expr.get()] = lc;
  accum_child_cost_[new_expr.get()] = 0;

  if (limit && lc >= *limit) return;

  Limit child_limit = limit ? std::optional{*limit - lc} : std::nullopt;
  tasks_.emplace([this, new_expr, child_limit]() { OptimizeInputs(new_expr, child_limit); });
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::OptimizeExpression(
    utils::NotNull<LogicalExpr*> expr, Limit limit) {
  Log("Optimizing expression in group {}", expr->group->GetId());
  for (size_t rule = 0; rule < NImplementation; rule++) {
    if (!rules_applier_.IsApplicable(ImplementationRuleId{rule}, expr)) {
      continue;
    }
    tasks_.emplace([this, expr, rule, limit]() {
      ApplyRule(ImplementationRuleId{rule}, expr, limit);
    });
  }

  for (auto child : GetChildren(expr)) {
    if (IsExplored(child)) {
      continue;
    }
    tasks_.emplace([this, child, limit]() {
      ExploreGroup(child, limit);
    });
  }
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::ExploreExpression(
    utils::NotNull<LogicalExpr*> expr, Limit limit) {
  Log("Exploring expression in group {}", expr->group->GetId());
  for (size_t rule = 0; rule < NTransformation; rule++) {
    if (!rules_applier_.IsApplicable(TransformationRuleId{rule}, expr)) {
      continue;
    }
    tasks_.emplace([this, expr, rule, limit]() {
      ApplyRule(TransformationRuleId{rule}, expr, limit);
    });
  }

  for (auto child : GetChildren(expr)) {
    if (IsExplored(child)) {
      continue;
    }
    tasks_.emplace([this, child, limit]() {
      ExploreGroup(child, limit);
    });
  }
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::ExploreGroup(
    utils::NotNull<Group*> group, Limit limit) {
  Log("Exploring group {}", group->GetId());
  SetExplored(group);
  for (auto expr : group->GetLogicalExprs()) {
    tasks_.emplace([this, expr, limit]() {
      ExploreExpression(expr, limit);
    });
  }
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::OptimizeGroup(
    utils::NotNull<Group*> group, Limit limit) {
  Log("Optimizing group {}", group->GetId());
  if (auto it = best_cost_.find(group.get()); it != best_cost_.end()) {
    if (!limit || it->second < *limit) return;
  }

  if (!IsExplored(group)) {
    tasks_.emplace([this, group, limit]() { OptimizeGroup(group, limit); });
    tasks_.emplace([this, group, limit]() { ExploreGroup(group, limit); });
    return;
  }

  for (auto expr : group->GetLogicalExprs()) {
    tasks_.emplace([this, expr, limit]() { OptimizeExpression(expr, limit); });
  }
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::BuildOptimalPlan(Group* group) {
  Log("Building optimal plan for group {}", group->GetId());
  auto best_expr = best_plan_[group];
  if (!best_expr) {
    throw std::runtime_error{"no optimal plan for group"};
  }
  return std::visit(
      utils::Overloaded{
          [](const physical::SeqScan& op) -> PhysicalPlanNode {
            return SeqScan{.table = op.table};
          },
          [this](const physical::Projection& op) -> PhysicalPlanNode {
            return PhysicalProjection{
                .source = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.source.get())),
                .expressions = op.expressions,
            };
          },
          [this](const physical::Filter& op) -> PhysicalPlanNode {
            return PhysicalFilter{
                .source = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.source.get())),
                .predicate = op.predicate,
            };
          },
          [this](const physical::NestedLoopJoin& op) -> PhysicalPlanNode {
            return NestedLoopJoin{
                .lhs = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.lhs.get())),
                .rhs = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.rhs.get())),
                .type = op.type,
                .qual = op.qual,
            };
          },
          [this](const physical::NestedLoopCrossJoin& op) -> PhysicalPlanNode {
            return NestedLoopCrossJoin{
                .lhs = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.lhs.get())),
                .rhs = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.rhs.get())),
            };
          },
      },
      best_expr->root_operator);
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::Optimize() {
  Log("Starting optimization");
  tasks_.emplace([this]() { OptimizeGroup(root_->group); });
  while (!tasks_.empty()) {
    auto next_task = std::move(tasks_.top());
    tasks_.pop();
    next_task();
  }
  Log("Optimization complete, building plan");
  return BuildOptimalPlan(root_->group.get());
}

template class Optimizer<2, 5>;

}  // namespace stewkk::sql
