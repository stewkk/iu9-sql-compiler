#include <stewkk/sql/logic/optimizer/optimizer.hpp>

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <limits>

#include <bit>

#include <boost/container_hash/hash.hpp>

#include <stewkk/sql/utils/overloaded.hpp>
#include <stewkk/sql/utils/log.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_index_seek.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>
#include <stewkk/sql/logic/optimizer/sort_enforcer.hpp>

namespace stewkk::sql {

namespace {

static const std::vector<std::unique_ptr<Enforcer>> kEnforcers = [] {
  std::vector<std::unique_ptr<Enforcer>> v;
  v.push_back(std::make_unique<SortEnforcer>());
  return v;
}();


PropertySet RequiredInputProps(utils::NotNull<PhysicalExpr*> expr,
                                      PropertySet required, size_t child_index) {
  return std::visit(utils::Overloaded{
      [&](const physical::SeqScan&) { return PropertySet::Any(); },
      [&](const physical::IndexSeek&) { return PropertySet::Any(); },
      [&](const physical::Filter&) { return required; },
      [&](const physical::Projection&) { return required; },
      [&](const physical::NestedLoopJoin&) { return PropertySet::Any(); },
      [&](const physical::NestedLoopCrossJoin&) { return PropertySet::Any(); },
      [&](const physical::HashJoin&) -> PropertySet {
          return PropertySet::Any();
      },
      [&](const physical::Sort&) { return PropertySet::Any(); },
      [&](const physical::Aggregation&) { return PropertySet::Any(); },
  }, expr->root_operator);
}

PropertySet DeriveOutputProps(utils::NotNull<PhysicalExpr*> expr,
                                     const std::vector<PropertySet>& child_delivered) {
  return std::visit(utils::Overloaded{
      [&](const physical::SeqScan&) { return PropertySet::Any(); },
      [&](const physical::IndexSeek&) { return PropertySet::Any(); },
      [&](const physical::Filter&) { return child_delivered[0]; },
      [&](const physical::Projection&) { return child_delivered[0]; },
      [&](const physical::NestedLoopJoin&) { return PropertySet::Any(); },
      [&](const physical::NestedLoopCrossJoin&) { return PropertySet::Any(); },
      [&](const physical::HashJoin&) { return PropertySet::Any(); },
      [&](const physical::Sort& s) { return PropertySet{SortProperty{s.keys}}; },
      [&](const physical::Aggregation&) { return PropertySet::Any(); },
  }, expr->root_operator);
}

int64_t HashJoinCost(utils::NotNull<Group*> build, utils::NotNull<Group*> probe,
                     utils::NotNull<Group*> output, CardinalityEstimates& cardinality,
                     SchemaCatalog& schema) {
  constexpr int64_t kHashBuild = 100;
  constexpr int64_t kHashProbe = 35;
  constexpr int64_t kTupleCopy = 10;
  return kHashBuild * cardinality.GetCardinality(build) * schema.GetWidth(build)
       + kHashProbe * cardinality.GetCardinality(probe)
       + kTupleCopy * cardinality.GetCardinality(output) * schema.GetWidth(output);
}

int64_t LowerBoundLocalCost(utils::NotNull<LogicalExpr*> expr, CardinalityEstimates& cardinality,
                            SchemaCatalog& schema) {
  return std::visit(utils::Overloaded{
      [&](const logical::Table&) -> int64_t {
          return 100 * cardinality.GetCardinality(expr->group);
      },
      [&](const logical::Filter&) -> int64_t {
          return 100 * cardinality.GetCardinality(expr->group);
      },
      [&](const logical::Projection&) -> int64_t {
          return 22 * cardinality.GetCardinality(expr->group);
      },
      [&](const logical::Aggregation& a) -> int64_t {
          return 510 * cardinality.GetCardinality(a.source);
      },
      [&](const logical::CrossJoin& j) -> int64_t {
          return 104 * cardinality.GetCardinality(j.lhs) * cardinality.GetCardinality(j.rhs);
      },
      [&](const logical::Join& j) -> int64_t {
          auto n_l = cardinality.GetCardinality(j.lhs);
          auto n_r = cardinality.GetCardinality(j.rhs);
          return std::min({
              HashJoinCost(j.lhs, j.rhs, expr->group, cardinality, schema),
              HashJoinCost(j.rhs, j.lhs, expr->group, cardinality, schema),
              70 * n_l * n_r,
          });
      },
  }, expr->root_operator);
}

int64_t CalcCost(utils::NotNull<PhysicalExpr*> expr, CardinalityEstimates& cardinality,
                 SchemaCatalog& schema) {
  return std::visit(utils::Overloaded{
      [&](const physical::SeqScan&) -> int64_t {
          return 100 * cardinality.GetCardinality(expr->group);
      },
      [&](const physical::IndexSeek&) -> int64_t {
          auto out = cardinality.GetCardinality(expr->group);
          return 200 * out + 100 * static_cast<int64_t>(
              std::bit_width(static_cast<uint64_t>(std::max<int64_t>(1, out))));
      },
      [&](const physical::Filter&) -> int64_t {
          return 100 * cardinality.GetCardinality(expr->group);
      },
      [&](const physical::Projection&) -> int64_t {
          return 22 * cardinality.GetCardinality(expr->group);
      },
      [&](const physical::NestedLoopJoin& j) -> int64_t {
          auto n_l = cardinality.GetCardinality(j.lhs);
          auto n_r = cardinality.GetCardinality(j.rhs);
          return 70 * n_l * n_r;
      },
      [&](const physical::NestedLoopCrossJoin& j) -> int64_t {
          auto n_l = cardinality.GetCardinality(j.lhs);
          auto n_r = cardinality.GetCardinality(j.rhs);
          return 104 * n_l * n_r;
      },
      [&](const physical::HashJoin& j) -> int64_t {
          return HashJoinCost(j.lhs, j.rhs, expr->group, cardinality, schema);
      },
      [&](const physical::Sort& s) -> int64_t {
          auto n = cardinality.GetCardinality(s.input);
          return 11 * (n > 1 ? n * static_cast<int64_t>(std::bit_width(static_cast<uint64_t>(n))) : n);
      },
      [&](const physical::Aggregation& a) -> int64_t {
          return 510 * cardinality.GetCardinality(a.source);
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
      [](const logical::Aggregation& a) -> std::vector<utils::NotNull<Group*>> {
          return {a.source};
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
      [](const physical::IndexSeek&) -> std::vector<utils::NotNull<Group*>> {
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
      [](const physical::HashJoin& j) -> std::vector<utils::NotNull<Group*>> {
          return {j.lhs, j.rhs};
      },
      [](const physical::Sort& s) -> std::vector<utils::NotNull<Group*>> {
          return {s.input};
      },
      [](const physical::Aggregation& a) -> std::vector<utils::NotNull<Group*>> {
          return {a.source};
      },
  }, expr->root_operator);
}

template<size_t NTransformation, size_t NImplementation>
Optimizer<NTransformation, NImplementation>::Optimizer(
    const Operator& expr, Rules<NTransformation, NImplementation>&& rules,
    CardinalityEstimates cardinality, SchemaCatalog schema, PropertySet required)
    : memo_(), rules_applier_(std::move(rules)), root_(memo_.Populate(expr)),
      cardinality_(std::move(cardinality)), schema_(std::move(schema)),
      global_required_(std::move(required)) {
}

template<size_t NTransformation, size_t NImplementation>
int64_t Optimizer<NTransformation, NImplementation>::LowerBoundCost(utils::NotNull<Group*> group) {
  if (auto it = lower_bounds_.find(group.get()); it != lower_bounds_.end()) {
    return it->second;
  }
 
  int64_t best = std::numeric_limits<int64_t>::max();
  for (auto expr : group->GetLogicalExprs()) {
    int64_t local = LowerBoundLocalCost(expr, cardinality_, schema_);
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
 
 
  WinnerKey self_key{expr->group.get(), required};
  if (auto it = winner_.find(self_key); it != winner_.end()) {
    limit = limit ? Limit{std::min(*limit, it->second.cost)} : Limit{it->second.cost};
  }
  if (limit && accum >= *limit) return;
  auto children = GetChildren(expr);
  if (child_index >= children.size()) {
    auto delivered = DeriveOutputProps(expr, child_delivered);
    if (!delivered.Satisfies(required)) return;
    WinnerKey key{expr->group.get(), required};
    if (!winner_.contains(key) || accum < winner_.at(key).cost) {
      Log("New best plan for group {} with cost {}", expr->group->GetId(), accum);
      winner_[key] = WinnerEntry{accum, expr.get(), delivered};
    }
    return;
  }

  auto child = children[child_index];
  auto child_required = RequiredInputProps(expr, required, child_index);

  tasks_.emplace([this, expr, child, child_index, required, child_delivered, accum, limit, child_required]() mutable {
    WinnerKey child_key{child.get(), child_required};
    auto child_it = winner_.find(child_key);
    if (child_it == winner_.end()) return;
    auto new_accum = accum + child_it->second.cost;
    if (limit && new_accum >= *limit) return;
    child_delivered.push_back(child_it->second.delivered);
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
      auto lc = CalcCost(new_expr, cardinality_, schema_);
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
  if (winner_.contains(key)) return;

  if (IsExplored(group) && limit && LowerBoundCost(group) >= *limit) return;

  if (!IsExplored(group)) {
    tasks_.emplace([this, group, required, limit]() {
      OptimizeGroup(group, required, limit);
    });
    tasks_.emplace([this, group, limit]() { ExploreGroup(group, limit); });
    return;
  }

 
 
 
  for (auto phys_expr : group->GetPhysicalExprs()) {
    if (phys_expr->is_enforcer) continue;
    auto lc = local_cost_[phys_expr.get()];
    if (limit && lc >= *limit) continue;
    tasks_.emplace([this, phys_expr, required, lc, limit]() {
      OptimizeInputs(phys_expr, required, {}, lc, limit);
    });
  }

  if (!enforcers_added_.contains(key)) {
    enforcers_added_.insert(key);
    for (const auto& enforcer : kEnforcers) {
      auto op = enforcer->TryBuild(group, required, schema_);
      if (!op) continue;
      auto enf_expr = group->AddPhysicalExpr(*op, true);
      auto lc = CalcCost(enf_expr, cardinality_, schema_);
      Log("Enforcer local cost for group {}: {}", group->GetId(), lc);
      local_cost_[enf_expr.get()] = lc;
      if (!limit || lc < *limit) {
        tasks_.emplace([this, enf_expr, required, lc, limit]() {
          OptimizeInputs(enf_expr, required, {}, lc, limit);
        });
      }
    }
  }
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::BuildOptimalPlan(Group* group, PropertySet required) {
  Log("Building optimal plan for group {}", group->GetId());
  WinnerKey key{group, required};
  auto it = winner_.find(key);
  if (it == winner_.end() || !it->second.plan) {
    throw std::runtime_error{"no optimal plan for group"};
  }
  auto* best_expr = it->second.plan;
  utils::NotNull<PhysicalExpr*> best_expr_nn{best_expr};
  auto plan = std::visit(
      utils::Overloaded{
          [](const physical::SeqScan& op) -> PhysicalPlanNode {
            return SeqScan{.table = op.table, .alias = op.alias};
          },
          [](const physical::IndexSeek& op) -> PhysicalPlanNode {
            return IndexSeek{.table = op.table, .alias = op.alias, .predicate = op.predicate};
          },
          [this, best_expr_nn, required](const physical::Projection& op) -> PhysicalPlanNode {
            return PhysicalProjection{
                .source = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.source.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .expressions = op.expressions,
                .aliases = op.aliases,
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
          [this, best_expr_nn, required](const physical::HashJoin& op) -> PhysicalPlanNode {
            return HashJoin{
                .lhs = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.lhs.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .rhs = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.rhs.get(), RequiredInputProps(best_expr_nn, required, 1))),
                .type = op.type,
                .qual = op.qual,
            };
          },
          [this, best_expr_nn, required](const physical::Sort& op) -> PhysicalPlanNode {
            return PhysicalSort{
                .source = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.input.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .keys = op.keys,
            };
          },
          [this, best_expr_nn, required](const physical::Aggregation& op) -> PhysicalPlanNode {
            return PhysicalAggregation{
                .source = std::make_shared<PhysicalPlanNode>(
                    BuildOptimalPlan(op.source.get(), RequiredInputProps(best_expr_nn, required, 0))),
                .group_by = op.group_by,
                .aggregates = op.aggregates,
            };
          },
      },
      best_expr->root_operator);
  plan.metadata = PlanNodeMetadata{
      .cardinality = cardinality_.GetCardinality(best_expr->group),
      .local_cost = local_cost_.at(best_expr),
  };
  return plan;
}

template<size_t NTransformation, size_t NImplementation>
void Optimizer<NTransformation, NImplementation>::RunSearch(Limit limit) {
  Log("Starting optimization");
  tasks_.emplace([this, limit]() { OptimizeGroup(root_->group, global_required_, limit); });
  while (!tasks_.empty()) {
    auto next_task = std::move(tasks_.top());
    tasks_.pop();
    next_task();
  }
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::Optimize() {
  const auto started = std::chrono::steady_clock::now();
  RunSearch(std::numeric_limits<int64_t>::max());
  Log("Optimization complete, building plan");
  auto plan = BuildOptimalPlan(root_->group.get(), global_required_);
  const auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - started);
  Log("Optimization finished in {} us, chosen plan cost {}", runtime.count(), GetBestCost());
  return plan;
}

template<size_t NTransformation, size_t NImplementation>
PhysicalPlanNode Optimizer<NTransformation, NImplementation>::OptimizeExhaustive() {
  const auto started = std::chrono::steady_clock::now();
  RunSearch(std::nullopt);
  auto plan = BuildOptimalPlan(root_->group.get(), global_required_);
  const auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - started);
  Log("Exhaustive optimization finished in {} us, chosen plan cost {}", runtime.count(),
      GetBestCost());
  return plan;
}

template<size_t NTransformation, size_t NImplementation>
std::int64_t Optimizer<NTransformation, NImplementation>::GetBestCost() const {
  WinnerKey key{root_->group.get(), global_required_};
  auto it = winner_.find(key);
  if (it == winner_.end()) {
    throw std::runtime_error{"no optimal plan cost"};
  }
  return it->second.cost;
}

template<size_t NTransformation, size_t NImplementation>
utils::NotNull<Group*> Optimizer<NTransformation, NImplementation>::GetRootGroup() const {
  return root_->group;
}

template class Optimizer<7, 8>;
template class Optimizer<0, 6>;

}  // namespace stewkk::sql
