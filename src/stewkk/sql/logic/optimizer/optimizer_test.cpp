#include <gmock/gmock.h>

#include <stack>
#include <unordered_map>
#include <ranges>
#include <unordered_set>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/utils/overloaded.hpp>
#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/optimizer/rules_applier.hpp>
#include <stewkk/sql/logic/executor/plan.hpp>
#include <stewkk/sql/logic/executor/buffer_size.hpp>
#include <stewkk/sql/logic/executor/plan_serializer.hpp>

using ::testing::Eq;

namespace stewkk::sql {
    
// FIXME: branch and bound
// FIXME: сделать API в виде DoStep(), которое возвращает какой-то внутренний стейт оптимизатора
// FIXME: применение правила (по крайней мере трансформации), должно создавать несколько выражений

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

using Limit = std::optional<std::int64_t>;

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

class CardinalityEstimates {
public:
  CardinalityEstimates(std::unordered_map<std::string, int64_t> table_sizes = {})
      : table_sizes_(std::move(table_sizes)) {}

  int64_t GetCardinality(utils::NotNull<Group*> group) {
    if (auto it = cache_.find(group.get()); it != cache_.end()) {
      return it->second;
    }
    auto cardinality = GetCardinality(group->GetLogicalExprs().front()->root_operator);
    cache_[group.get()] = cardinality;
    return cardinality;
  }

  private:
  int64_t GetCardinality(const LogicalOperator& op) {
    return std::visit(utils::Overloaded{
        [this](const logical::Table& t) -> int64_t {
            if (auto it = table_sizes_.find(t.name); it != table_sizes_.end()) {
                return it->second;
            }
            return 10;
        },
        [this](const logical::Filter& f) -> int64_t {
            return GetCardinality(f.source);
        },
        [this](const logical::Projection& p) -> int64_t {
            return GetCardinality(p.source);
        },
        [this](const logical::CrossJoin& j) -> int64_t {
            return GetCardinality(j.lhs) * GetCardinality(j.rhs);
        },
        [this](const logical::Join& j) -> int64_t {
            return GetCardinality(j.lhs) * GetCardinality(j.rhs);
        },
    }, op);
  }

  std::unordered_map<std::string, int64_t> table_sizes_;
  std::unordered_map<Group*, int64_t> cache_;
};

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
    }, expr->root_operator);
}

template<size_t NTransformation, size_t NImplementation>
class Optimizer {
    public:
      Optimizer(const Operator& expr, Rules<NTransformation, NImplementation>&& rules,
                CardinalityEstimates cardinality = {})
          : memo_(), rules_applier_(std::move(rules)), root_(memo_.Populate(expr)),
          cardinality_(std::move(cardinality)) {
      }

private:
      bool IsExplored(utils::NotNull<Group*> group) const {
        return explored_groups_.contains(group.get());
      }

      void SetExplored(utils::NotNull<Group*> group) {
        explored_groups_.insert(group);
      }

      void OptimizeInputs(utils::NotNull<PhysicalExpr*> expr, Limit limit, size_t child_index = 0) {
        auto children = GetChildren(expr);
        if (child_index >= children.size()) {
          int64_t total = local_cost_[expr.get()] + accum_child_cost_[expr.get()];
          auto* g = expr->group.get();
          if (!best_cost_.contains(g) || total < best_cost_.at(g)) {
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
          // FIXME add -LB(children[child_index+1..k])
          Limit next = limit ? std::optional{*limit - local_cost_[expr.get()] - accum_child_cost_[expr.get()]} : std::nullopt;
          if (next && *next < 0) return;
          OptimizeInputs(expr, next, child_index + 1);
        });

        tasks_.emplace([this, child, limit]() {
          OptimizeGroup(child, limit);
        });
      }

      void ApplyRule(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit) {
        auto new_expr = rules_applier_.Apply(rule, expr, memo_);
        tasks_.emplace([this, new_expr, limit]() { ExploreExpression(new_expr, limit); });
      }

      void ApplyRule(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr, Limit limit) {
        auto new_expr = rules_applier_.Apply(rule, expr, memo_);
        // FIXME: missing physical properties
        auto lc = CalcCost(new_expr, cardinality_);
        local_cost_[new_expr.get()] = lc;
        accum_child_cost_[new_expr.get()] = 0;

        if (limit && lc >= *limit) return;

        Limit child_limit = limit ? std::optional{*limit - lc} : std::nullopt;
        tasks_.emplace([this, new_expr, child_limit]() { OptimizeInputs(new_expr, child_limit); });
      }

      void OptimizeExpression(utils::NotNull<LogicalExpr*> expr, Limit limit) {
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

      void ExploreExpression(utils::NotNull<LogicalExpr*> expr, Limit limit) {
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

      void ExploreGroup(utils::NotNull<Group*> group, Limit limit) {
        SetExplored(group);
        for (auto expr : group->GetLogicalExprs()) {
          tasks_.emplace([this, expr, limit]() {
            ExploreExpression(expr, limit);
          });
        }
      }

      void OptimizeGroup(utils::NotNull<Group*> group, Limit limit=std::nullopt) {
        if (auto it = best_cost_.find(group.get()); it != best_cost_.end()) {
          if (!limit || it->second < *limit) return;
        }

        if (!IsExplored(group)) {
          tasks_.emplace([this, group, limit](){
            OptimizeGroup(group, limit);
          });
          tasks_.emplace([this, group, limit](){
            ExploreGroup(group, limit);
          });
          return;
        }

        for (auto expr : group->GetLogicalExprs()) {
          tasks_.emplace([this, expr, limit](){
            OptimizeExpression(expr, limit);
          });
        }
      }
      
      PhysicalPlanNode BuildOptimalPlan(Group* group) {
        auto best_expr = best_plan_[group];
        if (!best_expr) {
          throw std::runtime_error{"no optimal plan for group"};
        }
        return std::visit(
            utils::Overloaded{
                [](const physical::SeqScan& op) -> PhysicalPlanNode {
                  return SeqScan{
                      .table = op.table,
                  };
                },
                [this](const physical::Projection& op) -> PhysicalPlanNode {
                  return PhysicalProjection{
                      .source
                      = std::make_shared<PhysicalPlanNode>(BuildOptimalPlan(op.source.get())),
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

        public:
      PhysicalPlanNode Optimize() {
        tasks_.emplace([this]() {
          OptimizeGroup(root_->group);
        });
        while (!tasks_.empty()) {
          auto next_task = std::move(tasks_.top());
          tasks_.pop();
          next_task();
        }
        return BuildOptimalPlan(root_->group.get());
      }

    private:
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

TEST(OptimizerTest, Simple) {
  std::stringstream s{"SELECT * FROM users;"};
  Operator op = GetAST(s).value();
  Optimizer optimizer(op, MakeMainRules());

  auto got = optimizer.Optimize();
  
  ASSERT_THAT(SerializeDot(got), Eq("digraph G { rankdir=BT;\n  n0 [label=\"SeqScan\\\\nusers\"]\n}\n"));
}

TEST(OptimizerTest, JoinCommutativity) {
  std::stringstream s{"SELECT * FROM users JOIN orders ON users.id = orders.user_id;"};
  Operator op = GetAST(s).value();
  Optimizer optimizer(op, MakeMainRules(), CardinalityEstimates({
      {"users", 10000},
      {"orders", 100},
  }));

  auto got = optimizer.Optimize();

  ASSERT_THAT(Serialize(got), ::testing::HasSubstr("(SeqScan orders) (SeqScan users)"));
}

}  // namespace stewkk::sql
