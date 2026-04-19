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

using ::testing::Eq;

namespace stewkk::sql {

//   GroupKey AddGroup(const LogicalExpr& expr, const Rules& rules) {
//     return std::visit(Overloaded{
//       [this, &rules](const logical::Table& t) {
//         auto key = std::format("Table({})", t.name);
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(t, rules, key)));
//         return key;
//       },
//       [this, &rules](const logical::Filter& f) {
//         auto key = std::format("Filter({}, {})", f.source, ToString(f.predicate));
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(f, rules, key)));
//         return key;
//       },
//       [this, &rules](const logical::Projection& p) {
//         auto exprs = p.expressions | std::views::transform([](const Expression& e) { return ToString(e); }) | std::views::join_with(',') | std::ranges::to<std::string>();
//         auto key = std::format("Projection({}, {})", p.source, exprs);
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(p, rules, key)));
//         return key;
//       },
//       [this, &rules](const logical::CrossJoin& j) {
//         auto key = std::format("CrossJoin({}, {})", j.lhs, j.rhs);
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(j, rules, key)));
//         return key;
//       },
//       [this, &rules](const logical::Join& j) {
//         auto key = std::format("Join({}, {}, {}, {})", j.lhs, j.rhs, ToString(j.type), ToString(j.qual));
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(j, rules, key)));
//         return key;
//       },
//     }, expr);
//   }
//   private:

//   GroupKey AddGroup(const Operator& expr, const Rules& rules) {
//     return std::visit(Overloaded{
//       [this, &rules](const Table& t) {
//         auto key = std::format("Table({})", t.name);
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Table(t), rules, key)));
//         return key;
//       },
//       [this, &rules](const Filter& f) {
//         auto source_group = AddGroup(*f.source, rules);
//         auto key = std::format("Filter({}, {})", source_group, ToString(f.expr));
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Filter(source_group, f.expr), rules, key)));
//         return key;
//       },
//       [this, &rules](const Projection& p) {
//         auto source_group = AddGroup(*p.source, rules);
//         auto exprs = p.expressions | std::views::transform([](const Expression& e) { return ToString(e); }) | std::views::join_with(',') | std::ranges::to<std::string>();
//         auto key = std::format("Projection({}, {})", source_group, exprs);
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Projection(source_group, p.expressions), rules, key)));
//         return key;
//       },
//       [this, &rules](const CrossJoin& j) {
//         auto lhs_group = AddGroup(*j.lhs, rules);
//         auto rhs_group = AddGroup(*j.rhs, rules);
//         auto key = std::format("CrossJoin({}, {})", lhs_group, rhs_group);
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::CrossJoin(lhs_group, rhs_group), rules, key)));
//         return key;
//       },
//       [this, &rules](const Join& j) {
//         auto lhs_group = AddGroup(*j.lhs, rules);
//         auto rhs_group = AddGroup(*j.rhs, rules);
//         auto key = std::format("Join({}, {}, {}, {})", lhs_group, rhs_group, ToString(j.type), ToString(j.qual));
//         mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Join(lhs_group, rhs_group, j.type, j.qual), rules, key)));
//         return key;
//       },
//     }, expr);
//   }

// class Optimizer {
//     public:

//     explicit Optimizer(const Operator& expr, const Rules& rules) : memo_(expr, rules), rules_count_(rules.Count()) {}

//     bool IsExplored(const Group& group) const {
//       return explored_groups_.contains(group.GetKey());
//     }

//     void SetExplored(const Group& group) {
//       explored_groups_.insert(group.GetKey());
//     }

//     void OptimizeInputs(const ExpressionRulesApplier& expr) {
//     }

//     void ApplyRule(ExpressionRulesApplier& expr, RuleNumber rule) {
//       auto new_expr = expr.ApplyRule(rule, memo_);
//       auto group = memo_.GetGroup(new_expr.GetGroup());
//       auto new_expr_ref = group.AddExpression(std::move(new_expr));
//       if (new_expr.IsTransformationRule(rule)) {
//         tasks_.emplace([this, &new_expr_ref]() { ExploreExpression(new_expr_ref); });
//       } else {
//         tasks_.emplace([this, &new_expr_ref]() { OptimizeInputs(new_expr_ref); });
//       }
//     }

//     void OptimizeExpression(ExpressionRulesApplier& expr) {
//       for (RuleNumber rule = 0; rule < rules_count_; rule++) {
//         if (!expr.IsApplicable(rule, memo_)) {
//           continue;
//         }
//         tasks_.emplace([this, &expr, rule]() {
//             ApplyRule(expr, rule);
//         });
//       }

//       for (auto child : expr.GetChildren()) {
//         auto& group = memo_.GetGroup(child);
//         if (IsExplored(group)) {
//           continue;
//         }
//         tasks_.emplace([this, &group]() {
//           ExploreGroup(group);
//         });
//       }
//     }

//     void ExploreExpression(ExpressionRulesApplier& expr) {
//       for (RuleNumber rule = 0; rule < rules_count_; rule++) {
//         bool transformation_rules_only = true;
//         if (!expr.IsApplicable(rule, memo_, transformation_rules_only)) {
//           continue;
//         }
//         tasks_.emplace([this, &expr, rule]() {
//             ApplyRule(expr, rule);
//         });
//       }

//       for (auto child : expr.GetChildren()) {
//         auto& group = memo_.GetGroup(child);
//         if (IsExplored(group)) {
//           continue;
//         }
//         tasks_.emplace([this, &group]() {
//           ExploreGroup(group);
//         });
//       }
//     }

//     void ExploreGroup(Group& group) {
//       SetExplored(group);
//       for (auto& expr : group.GetExpressions()) {
//         tasks_.emplace([this, &expr]() {
//           ExploreExpression(expr);
//         });
//       }
//     }

//     void OptimizeGroup(Group& group) {
//       if (!IsExplored(group)) {
//         tasks_.emplace([this, &group](){
//           OptimizeGroup(group);
//         });
//         tasks_.emplace([this, &group](){
//           ExploreGroup(group);
//         });
//         return;
//       }

//       for (auto& expr : group.GetExpressions()) {
//         tasks_.emplace([this, &expr](){
//           OptimizeExpression(expr);
//         });
//       }
//     }

//     Operator Optimize() {
//       tasks_.emplace([this]() {
//         OptimizeGroup(memo_.GetRoot());
//       });
//       while (!tasks_.empty()) {
//         auto next_task = std::move(tasks_.top());
//         tasks_.pop();
//         next_task();
//       }
//       return Table{"A"};
//     }

//     private:
//         Memo memo_;
//         std::stack<std::function<void()>> tasks_;
//         std::unordered_set<GroupKey> explored_groups_;
//         const size_t rules_count_;
// };

// TEST(OptimizerTest, Simple) {
//   std::stringstream s{"SELECT * FROM users;"};
//   Operator op = GetAST(s).value();
//   Optimizer optimizer(op, Rules{{}});

//   auto got = optimizer.Optimize();
// }

}  // namespace stewkk::sql
