#include <stewkk/sql/logic/optimizer/optimizer.hpp>

#include <algorithm>
#include <stdexcept>
#include <limits>

#include <bit>

#include <boost/container_hash/hash.hpp>

#include <stewkk/sql/utils/overloaded.hpp>
#include <stewkk/sql/utils/log.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>

namespace stewkk::sql {

namespace {

// Top-down: given `required` of this expr, what must child `child_index` deliver?
PropertySet RequiredInputProps(utils::NotNull<PhysicalExpr*> expr,
                                      PropertySet required, size_t child_index) {
  return std::visit(utils::Overloaded{
      [&](const physical::SeqScan&) { return PropertySet::Any(); },
      [&](const physical::Filter&) { return required; },
      [&](const physical::Projection&) { return required; },
      [&](const physical::NestedLoopJoin&) -> PropertySet {
          return child_index == 0 ? required : PropertySet::Any();
      },
      [&](const physical::NestedLoopCrossJoin&) -> PropertySet {
          return child_index == 0 ? required : PropertySet::Any();
      },
      [&](const physical::Sort&) { return PropertySet::Any(); },
  }, expr->root_operator);
}

// Bottom-up: given what children delivered, what does this expr deliver?
// Sort property is schema-blind — operators that need column access (sort
// enforcer placement, merge join applicability) check the schema separately.
PropertySet DeriveOutputProps(utils::NotNull<PhysicalExpr*> expr,
                                     const std::vector<PropertySet>& child_delivered) {
  return std::visit(utils::Overloaded{
      [&](const physical::SeqScan&) { return PropertySet::Any(); },
      [&](const physical::Filter&) { return child_delivered[0]; },
      [&](const physical::Projection&) { return child_delivered[0]; },
      [&](const physical::NestedLoopJoin&) { return child_delivered[0]; },
      [&](const physical::NestedLoopCrossJoin&) { return child_delivered[0]; },
      [&](const physical::Sort& s) { return PropertySet{s.keys}; },
  }, expr->root_operator);
}

// Best-case local cost achievable by any physical impl of this logical
// operator, ignoring required physical properties. Used to compute group lower
// bounds for B&B pruning. Must remain ≤ every CalcCost over physical impls of
// the same logical alternative — update when adding cheaper physical impls.
int64_t LowerBoundLocalCost(utils::NotNull<LogicalExpr*> expr, CardinalityEstimates& cardinality) {
  return std::visit(utils::Overloaded{
      [&](const logical::Table&) -> int64_t {
          return cardinality.GetCardinality(expr->group);
      },
      [&](const logical::Filter&) -> int64_t {
          return cardinality.GetCardinality(expr->group);
      },
      [&](const logical::Projection&) -> int64_t {
          return cardinality.GetCardinality(expr->group);
      },
      [&](const logical::CrossJoin& j) -> int64_t {
          auto p_l = (cardinality.GetCardinality(j.lhs) + kBufSize - 1) / kBufSize;
          auto p_r = (cardinality.GetCardinality(j.rhs) + kBufSize - 1) / kBufSize;
          return p_l * (1 + p_r);
      },
      [&](const logical::Join& j) -> int64_t {
          auto p_l = (cardinality.GetCardinality(j.lhs) + kBufSize - 1) / kBufSize;
          auto p_r = (cardinality.GetCardinality(j.rhs) + kBufSize - 1) / kBufSize;
          return p_l * (1 + p_r);
      },
  }, expr->root_operator);
}

int64_t CalcCost(utils::NotNull<PhysicalExpr*> expr, CardinalityEstimates& cardinality) {
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
      [&](const physical::Sort& s) -> int64_t {
          auto n = cardinality.GetCardinality(s.input);
          return n > 1 ? n * static_cast<int64_t>(std::bit_width(static_cast<uint64_t>(n))) : n;
      },
  }, expr->root_operator);
}

} // namespace

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
      [](const physical::Sort& s) -> std::vector<utils::NotNull<Group*>> {
          return {s.input};
      },
  }, expr->root_operator);
}

template<size_t NTransformation, size_t NImplementation>
Optimizer<NTransformation, NImplementation>::Optimizer(
    const Operator& expr, Rules<NTransformation, NImplementation>&& rules,
    CardinalityEstimates cardinality, SchemaCatalog schema)
    : memo_(), rules_applier_(std::move(rules)), root_(memo_.Populate(expr)),
      cardinality_(std::move(cardinality)), schema_(std::move(schema)) {}

template<size_t NTransformation, size_t NImplementation>
int64_t Optimizer<NTransformation, NImplementation>::LowerBoundCost(utils::NotNull<Group*> group) {
  if (auto it = lower_bounds_.find(group.get()); it != lower_bounds_.end()) {
    return it->second;
  }
  // Memo groups form a DAG (no cycles), so recursion terminates.
  int64_t best = std::numeric_limits<int64_t>::max();
  for (auto expr : group->GetLogicalExprs()) {
    int64_t local = LowerBoundLocalCost(expr, cardinality_);
    int64_t children = 0;
    bool overflow = false;
    for (auto child : GetChildren(expr)) {
      auto cb = LowerBoundCost(child);
      if (children > std::numeric_limits<int64_t>::max() - cb) { overflow = true; break; }
      children += cb;
    }
    if (overflow) continue;
    if (local > std::numeric_limits<int64_t>::max() - children) continue;
    best = std::min(best, local + children);
  }
  lower_bounds_[group.get()] = best;
  return best;
}

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
    utils::NotNull<PhysicalExpr*> expr, PropertySet required,
    std::vector<PropertySet> child_delivered, int64_t accum, Limit limit, size_t child_index) {
  // accum = local_cost(expr) + cost of children processed so far.
  // child_delivered[i] = what child i actually delivered (filled as we go).
  WinnerKey self_key{expr->group.get(), required};
  if (auto it = best_cost_.find(self_key); it != best_cost_.end()) {
    limit = limit ? Limit{std::min(*limit, it->second)} : Limit{it->second};
  }
  if (limit && accum >= *limit) return;
  auto children = GetChildren(expr);
  if (child_index >= children.size()) {
    auto delivered = DeriveOutputProps(expr, child_delivered);
    if (!delivered.Satisfies(required)) return;
    WinnerKey key{expr->group.get(), required};
    if (!best_cost_.contains(key) || accum < best_cost_.at(key)) {
      Log("New best plan for group {} with cost {}", expr->group->GetId(), accum);
      best_cost_[key] = accum;
      best_plan_[key] = expr.get();
      best_delivered_[key] = delivered;
    }
    return;
  }

  auto child = children[child_index];
  auto child_required = RequiredInputProps(expr, required, child_index);

  tasks_.emplace([this, expr, child, child_index, required, child_delivered, accum, limit, child_required]() mutable {
    WinnerKey child_key{child.get(), child_required};
    if (!best_cost_.contains(child_key)) return;
    auto new_accum = accum + best_cost_.at(child_key);
    if (limit && new_accum >= *limit) return;
    child_delivered.push_back(best_delivered_.at(child_key));
    OptimizeInputs(expr, required, std::move(child_delivered), new_accum, limit, child_index + 1);
  });

  int64_t children_lb = 0;
  for (size_t i = child_index + 1; i < children.size(); i++) {
    auto cb = LowerBoundCost(children[i]);
    if (children_lb > std::numeric_limits<int64_t>::max() - cb) { children_lb = std::numeric_limits<int64_t>::max(); break; }
    children_lb += cb;
  }
  Limit child_limit = limit ? Limit{*limit - accum - children_lb} : std::nullopt;
  tasks_.emplace([this, child, child_required, child_limit]() {
    OptimizeGroup(child, child_required, child_limit);
  });
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::ApplyRule(
    TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit) {
  Log("Applying transformation rule {} to group {}", rule.value, expr->group->GetId());
  auto new_expr = rules_applier_.Apply(rule, expr, memo_);
  tasks_.emplace([this, new_expr, limit]() { ExploreExpression(new_expr, limit); });
  // A new logical alternative in new_expr->group may unlock parent-side rules
  // whose patterns inspect child operators. Re-try rules on every parent;
  // already-applied (expr, rule) pairs are no-ops thanks to RulesApplier gating.
  if (auto it = group_parents_.find(new_expr->group.get()); it != group_parents_.end()) {
    for (auto* parent : it->second) {
      tasks_.emplace([this, parent, limit]() {
        TryRules(utils::NotNull<LogicalExpr*>{parent}, limit);
      });
    }
  }
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::TryRules(
    utils::NotNull<LogicalExpr*> expr, Limit limit) {
  for (size_t rule = 0; rule < NTransformation; rule++) {
    if (!rules_applier_.IsApplicable(TransformationRuleId{rule}, expr)) {
      continue;
    }
    tasks_.emplace([this, expr, rule, limit]() {
      ApplyRule(TransformationRuleId{rule}, expr, limit);
    });
  }

  for (size_t rule = 0; rule < NImplementation; rule++) {
    if (!rules_applier_.IsApplicable(ImplementationRuleId{rule}, expr)) {
      continue;
    }
    tasks_.emplace([this, expr, rule]() {
      Log("Applying implementation rule {} to group {}", rule, expr->group->GetId());
      auto new_expr = rules_applier_.Apply(ImplementationRuleId{rule}, expr, memo_);
      auto lc = CalcCost(new_expr, cardinality_);
      Log("Local cost for group {} expression: {}", new_expr->group->GetId(), lc);
      local_cost_[new_expr.get()] = lc;
    });
  }
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::ExploreExpression(
    utils::NotNull<LogicalExpr*> expr, Limit limit) {
  Log("Exploring expression in group {}", expr->group->GetId());
  TryRules(expr, limit);
  if (!explored_exprs_.insert(expr.get()).second) return;

  for (auto child : GetChildren(expr)) {
    group_parents_[child.get()].push_back(expr.get());
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
    utils::NotNull<Group*> group, PropertySet required, Limit limit) {
  Log("Optimizing group {}", group->GetId());
  WinnerKey key{group.get(), required};
  if (best_cost_.contains(key)) return;

  if (IsExplored(group) && limit && LowerBoundCost(group) >= *limit) return;

  if (!IsExplored(group)) {
    tasks_.emplace([this, group, required, limit]() {
      OptimizeGroup(group, required, limit);
    });
    tasks_.emplace([this, group, limit]() { ExploreGroup(group, limit); });
    return;
  }

  // Try every non-enforcer phys expr under `required`. Whether it actually
  // serves `required` is decided at the bottom of OptimizeInputs once children
  // have resolved and DeriveOutputProps can compute the true delivered.
  for (auto phys_expr : group->GetPhysicalExprs()) {
    if (phys_expr->is_enforcer) continue;
    auto lc = local_cost_[phys_expr.get()];
    if (limit && lc >= *limit) continue;
    tasks_.emplace([this, phys_expr, required, lc, limit]() {
      OptimizeInputs(phys_expr, required, {}, lc, limit);
    });
  }

  // Add Sort enforcer once per (group, required) if required asks for a sort,
  // and schedule its OptimizeInputs. Enforcers are scheduled here only — the
  // general scan skips them so the enforcer doesn't get picked up under
  // required=Any (which would recurse into its own group infinitely).
  // A Sort enforcer references the keys' columns by (table, name), so it is
  // only placeable when those columns are still in this group's output schema.
  // Unknown schema (catalog has no info) is treated permissively.
  if (required.sort && !enforcers_added_.contains(key)) {
    bool can_enforce = true;
    if (auto schema = schema_.GetSchema(group)) {
      for (const auto& sk : required.sort->keys) {
        bool found = false;
        for (const auto& a : *schema) {
          if (a.table == sk.table && a.name == sk.column) { found = true; break; }
        }
        if (!found) { can_enforce = false; break; }
      }
    }
    if (can_enforce) {
      enforcers_added_.insert(key);
      auto sort_expr = group->AddPhysicalExpr(
          physical::Sort{group, *required.sort}, /*is_enforcer=*/true);
      auto lc = CalcCost(sort_expr, cardinality_);
      Log("Sort enforcer local cost for group {}: {}", group->GetId(), lc);
      local_cost_[sort_expr.get()] = lc;
      if (!limit || lc < *limit) {
        tasks_.emplace([this, sort_expr, required, lc, limit]() {
          OptimizeInputs(sort_expr, required, {}, lc, limit);
        });
      }
    }
  }
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::BuildOptimalPlan(Group* group, PropertySet required) {
  Log("Building optimal plan for group {}", group->GetId());
  WinnerKey key{group, required};
  auto it = best_plan_.find(key);
  if (it == best_plan_.end() || !it->second) {
    throw std::runtime_error{"no optimal plan for group"};
  }
  auto* best_expr = it->second;
  utils::NotNull<PhysicalExpr*> best_expr_nn{best_expr};
  return std::visit(
      utils::Overloaded{
          [](const physical::SeqScan& op) -> PhysicalPlanNode {
            return SeqScan{.table = op.table};
          },
          [this, best_expr_nn, required](const physical::Projection& op) -> PhysicalPlanNode {
            return PhysicalProjection{
                .source = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.source.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .expressions = op.expressions,
            };
          },
          [this, best_expr_nn, required](const physical::Filter& op) -> PhysicalPlanNode {
            return PhysicalFilter{
                .source = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.source.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .predicate = op.predicate,
            };
          },
          [this, best_expr_nn, required](const physical::NestedLoopJoin& op) -> PhysicalPlanNode {
            return NestedLoopJoin{
                .lhs = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.lhs.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .rhs = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.rhs.get(), RequiredInputProps(best_expr_nn, required, 1))),
                .type = op.type,
                .qual = op.qual,
            };
          },
          [this, best_expr_nn, required](const physical::NestedLoopCrossJoin& op) -> PhysicalPlanNode {
            return NestedLoopCrossJoin{
                .lhs = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.lhs.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .rhs = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.rhs.get(), RequiredInputProps(best_expr_nn, required, 1))),
            };
          },
          [this, best_expr_nn, required](const physical::Sort& op) -> PhysicalPlanNode {
            return PhysicalSort{
                .source = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.input.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .keys = op.keys,
            };
          },
      },
      best_expr->root_operator);
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::RunSearch(PropertySet required, Limit limit) {
  Log("Starting optimization");
  tasks_.emplace([this, required, limit]() { OptimizeGroup(root_->group, required, limit); });
  while (!tasks_.empty()) {
    auto next_task = std::move(tasks_.top());
    tasks_.pop();
    next_task();
  }
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::Optimize(PropertySet required) {
  RunSearch(required, std::numeric_limits<int64_t>::max());
  Log("Optimization complete, building plan");
  return BuildOptimalPlan(root_->group.get(), required);
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::OptimizeExhaustive() {
  RunSearch(PropertySet::Any(), std::nullopt);
}

template<size_t NTransformation, size_t NImplementation>
utils::NotNull<Group*> Optimizer<NTransformation, NImplementation>::GetRootGroup() const {
  return root_->group;
}

template class Optimizer<2, 5>;

}  // namespace stewkk::sql
