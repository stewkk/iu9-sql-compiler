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

using ::testing::Eq;

namespace stewkk::sql {
    
// FIXME: проверить, не возникает ли висячих ссылок нигде?
// FIXME: branch and bound
// FIXME: физические операторы!
// FIXME: сделать API в виде DoStep(), которое возвращает какой-то внутренний стейт оптимизатора

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
  return {};
}

template<size_t NTransformation, size_t NImplementation>
class Optimizer {
    public:
      Optimizer(const Operator& expr, Rules<NTransformation, NImplementation>&& rules)
          : memo_(), rules_applier_(std::move(rules)), root_(memo_.Populate(expr)) {
      }

private:
      bool IsExplored(utils::NotNull<Group*> group) const {
        return explored_groups_.contains(group.get());
      }

      void SetExplored(utils::NotNull<Group*> group) {
        explored_groups_.insert(group);
      }

      void OptimizeInputs(utils::NotNull<PhysicalExpr*> expr, size_t child_index = 0) {
        auto children = GetChildren(expr);
        if (child_index >= children.size()) {
          // FIXME: update best plan
          return;
        }
        tasks_.emplace([this, expr, child_index]() {
          OptimizeInputs(expr, child_index+1);
        });
        tasks_.emplace([this, expr]() {
          OptimizeGroup(expr->group);
        });
      }

      void ApplyRule(TransformationRuleId rule, utils::NotNull<LogicalExpr*> expr) {
        auto new_expr = rules_applier_.Apply(rule, expr, memo_);
        tasks_.emplace([this, new_expr]() { ExploreExpression(new_expr); });
      }

      void ApplyRule(ImplementationRuleId rule, utils::NotNull<LogicalExpr*> expr) {
        auto new_expr = rules_applier_.Apply(rule, expr, memo_);
        tasks_.emplace(
            [this, new_expr]() { OptimizeInputs(new_expr); });
      }

      void OptimizeExpression(utils::NotNull<LogicalExpr*> expr) {
        for (size_t rule = 0; rule < NImplementation; rule++) {
          if (!rules_applier_.IsApplicable(ImplementationRuleId{rule}, expr)) {
            continue;
          }
          tasks_.emplace([this, expr, rule]() {
            ApplyRule(ImplementationRuleId{rule}, expr);
          });
        }

        for (auto child : GetChildren(expr)) {
          if (IsExplored(child)) {
            continue;
          }
          tasks_.emplace([this, child]() {
            ExploreGroup(child);
          });
        }
      }

      void ExploreExpression(utils::NotNull<LogicalExpr*> expr) {
        for (size_t rule = 0; rule < NTransformation; rule++) {
          if (!rules_applier_.IsApplicable(TransformationRuleId{rule}, expr)) {
            continue;
          }
          tasks_.emplace([this, expr, rule]() {
            ApplyRule(TransformationRuleId{rule}, expr);
          });
        }

        for (auto child : GetChildren(expr)) {
          if (IsExplored(child)) {
            continue;
          }
          tasks_.emplace([this, child]() {
            ExploreGroup(child);
          });
        }
      }

      void ExploreGroup(utils::NotNull<Group*> group) {
        SetExplored(group);
        for (auto expr : group->GetLogicalExprs()) {
          tasks_.emplace([this, expr]() {
            ExploreExpression(expr);
          });
        }
      }

      void OptimizeGroup(utils::NotNull<Group*> group) {
        if (!IsExplored(group)) {
          tasks_.emplace([this, group](){
            OptimizeGroup(group);
          });
          tasks_.emplace([this, group](){
            ExploreGroup(group);
          });
          return;
        }

        for (auto expr : group->GetLogicalExprs()) {
          tasks_.emplace([this, expr](){
            OptimizeExpression(expr);
          });
        }
      }

        public:
      Operator Optimize() {
        tasks_.emplace([this]() {
          OptimizeGroup(root_->group);
        });
        while (!tasks_.empty()) {
          auto next_task = std::move(tasks_.top());
          tasks_.pop();
          next_task();
        }
        return Table{"A"};
      }

    private:
        Memo memo_;
        RulesApplier<NTransformation, NImplementation> rules_applier_;
        std::stack<std::function<void()>> tasks_;
        std::unordered_set<Group*> explored_groups_;
        utils::NotNull<LogicalExpr*> root_;
};

TEST(OptimizerTest, Simple) {
  std::stringstream s{"SELECT * FROM users;"};
  Operator op = GetAST(s).value();
  Optimizer optimizer(op, MakeMainRules());

  auto got = optimizer.Optimize();
}

}  // namespace stewkk::sql
