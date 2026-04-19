#include <gmock/gmock.h>

#include <stack>
#include <unordered_map>
#include <ranges>
#include <unordered_set>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/result/result.hpp>

using ::testing::Eq;

namespace stewkk::sql {

template<class... Ts> struct Overloaded : Ts... { using Ts::operator()...; };

using GroupKey = std::string;

namespace logical {

using Table = Table;

struct Filter {
  GroupKey source;
  Expression predicate;
};

struct Projection {
  GroupKey source;
  std::vector<Expression> expressions;
};

struct CrossJoin {
  GroupKey lhs;
  GroupKey rhs;
};

struct Join {
  GroupKey lhs;
  GroupKey rhs;
  using Type = JoinType;
  Type type;
  Expression qual;
};

} // namespace logical

using LogicalExpr = std::variant<logical::Table, logical::Filter, logical::Projection, logical::Join, logical::CrossJoin>;

using RuleNumber = size_t;

class Memo;

class Rule {
  public:
    virtual bool IsApplicable(LogicalExpr expr, Memo& memo) = 0;
    virtual bool IsTransformationRule() const = 0;
    virtual LogicalExpr Apply(const LogicalExpr& expr, Memo& memo) = 0;
    virtual ~Rule() = default;
};

class Rules {
  public:

  explicit Rules(std::vector<std::unique_ptr<Rule>> rules) : rules_(std::move(rules)) {}

  size_t Count() const {
    return rules_.size();
  }

  LogicalExpr Apply(const LogicalExpr& expr, RuleNumber rule, Memo& memo) const {
    return rules_[rule]->Apply(expr, memo);
  }

  bool IsApplicable(RuleNumber rule, const LogicalExpr& expr, Memo& memo) const {
    return rules_[rule]->IsApplicable(expr, memo);
  }

  bool IsTransformationRule(RuleNumber rule) const {
    return rules_[rule]->IsTransformationRule();
  }

  private:
    std::vector<std::unique_ptr<Rule>> rules_;
};

class ExpressionRulesApplier {
  public:
    ExpressionRulesApplier(LogicalExpr&& expr, const Rules& rules, GroupKey group)
        : expr_(std::move(expr)),
          rules_(rules),
          is_applied_(rules_.Count(), false),
          group_(std::move(group)) {}

    ExpressionRulesApplier ApplyRule(RuleNumber rule, Memo& memo) {
      is_applied_[rule] = true;
      return ExpressionRulesApplier(rules_.Apply(expr_, rule, memo), rules_, group_);
    }

  bool IsApplicable(RuleNumber rule, Memo& memo, bool transformation_rules_only=false) const {
    return !IsApplied(rule) && (!transformation_rules_only || rules_.IsTransformationRule(rule)) && rules_.IsApplicable(rule, expr_, memo);
  }

    bool IsTransformationRule(RuleNumber rule) const {
      return rules_.IsTransformationRule(rule);
    }

  GroupKey GetGroup() const { return group_; }
  const LogicalExpr& GetExpr() const { return expr_; }

  std::vector<GroupKey> GetChildren() const {
    return std::visit(Overloaded{
      [](const logical::Table&)                       -> std::vector<GroupKey> { return {}; },
      [](const logical::Filter& f)                    -> std::vector<GroupKey> { return {f.source}; },
      [](const logical::Projection& p)                -> std::vector<GroupKey> { return {p.source}; },
      [](const logical::CrossJoin& j)                 -> std::vector<GroupKey> { return {j.lhs, j.rhs}; },
      [](const logical::Join& j)                      -> std::vector<GroupKey> { return {j.lhs, j.rhs}; },
    }, expr_);
  }

  private:
    bool IsApplied(RuleNumber rule) const { return is_applied_[rule]; }

    const LogicalExpr expr_;
    const Rules& rules_;
    GroupKey group_;
    std::vector<char> is_applied_;
};

class Group {
  public:
    Group(GroupKey key, ExpressionRulesApplier expr) : key_(std::move(key)), logical_exprs_({std::move(expr)}) {}
    const GroupKey& GetKey() const {
      return key_;
    }
    std::span<ExpressionRulesApplier> GetExpressions() {
      return logical_exprs_;
    }
    ExpressionRulesApplier& AddExpression(ExpressionRulesApplier expr) {
      logical_exprs_.emplace_back(std::move(expr));
      return logical_exprs_.back();
    }
  private:
    const GroupKey key_;
    std::vector<ExpressionRulesApplier> logical_exprs_;
};

class Memo {
 public:
  explicit Memo(const Operator& expr, const Rules& rules) : mapping_(), rules_(rules), root_(AddGroup(expr, rules)) {}

  Group& GetRoot() {
    return mapping_.find(root_)->second;
  }

  GroupKey AddGroup(const LogicalExpr& expr) {
    return AddGroup(expr, rules_);
  }

  GroupKey AddGroup(const LogicalExpr& expr, const Rules& rules) {
    return std::visit(Overloaded{
      [this, &rules](const logical::Table& t) {
        auto key = std::format("Table({})", t.name);
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(t, rules, key)));
        return key;
      },
      [this, &rules](const logical::Filter& f) {
        auto key = std::format("Filter({}, {})", f.source, ToString(f.predicate));
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(f, rules, key)));
        return key;
      },
      [this, &rules](const logical::Projection& p) {
        auto exprs = p.expressions | std::views::transform([](const Expression& e) { return ToString(e); }) | std::views::join_with(',') | std::ranges::to<std::string>();
        auto key = std::format("Projection({}, {})", p.source, exprs);
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(p, rules, key)));
        return key;
      },
      [this, &rules](const logical::CrossJoin& j) {
        auto key = std::format("CrossJoin({}, {})", j.lhs, j.rhs);
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(j, rules, key)));
        return key;
      },
      [this, &rules](const logical::Join& j) {
        auto key = std::format("Join({}, {}, {}, {})", j.lhs, j.rhs, ToString(j.type), ToString(j.qual));
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(j, rules, key)));
        return key;
      },
    }, expr);
  }

    // TODO: replace with raw ptr (or some non-null wrapper)
  Group& GetGroup(GroupKey key) {
    return mapping_.find(key)->second;
  }

  private:

  GroupKey AddGroup(const Operator& expr, const Rules& rules) {
    return std::visit(Overloaded{
      [this, &rules](const Table& t) {
        auto key = std::format("Table({})", t.name);
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Table(t), rules, key)));
        return key;
      },
      [this, &rules](const Filter& f) {
        auto source_group = AddGroup(*f.source, rules);
        auto key = std::format("Filter({}, {})", source_group, ToString(f.expr));
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Filter(source_group, f.expr), rules, key)));
        return key;
      },
      [this, &rules](const Projection& p) {
        auto source_group = AddGroup(*p.source, rules);
        auto exprs = p.expressions | std::views::transform([](const Expression& e) { return ToString(e); }) | std::views::join_with(',') | std::ranges::to<std::string>();
        auto key = std::format("Projection({}, {})", source_group, exprs);
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Projection(source_group, p.expressions), rules, key)));
        return key;
      },
      [this, &rules](const CrossJoin& j) {
        auto lhs_group = AddGroup(*j.lhs, rules);
        auto rhs_group = AddGroup(*j.rhs, rules);
        auto key = std::format("CrossJoin({}, {})", lhs_group, rhs_group);
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::CrossJoin(lhs_group, rhs_group), rules, key)));
        return key;
      },
      [this, &rules](const Join& j) {
        auto lhs_group = AddGroup(*j.lhs, rules);
        auto rhs_group = AddGroup(*j.rhs, rules);
        auto key = std::format("Join({}, {}, {}, {})", lhs_group, rhs_group, ToString(j.type), ToString(j.qual));
        mapping_.emplace(key, Group(key, ExpressionRulesApplier(logical::Join(lhs_group, rhs_group, j.type, j.qual), rules, key)));
        return key;
      },
    }, expr);
  }

  std::unordered_map<GroupKey, Group> mapping_;
  const Rules& rules_;
  GroupKey root_;
};

// DSU data-structure needed?

TEST(OptimizerTest, GetGroup) {
  Operator join_ab = Join{
      .type = JoinType::kInner,
      .qual = Literal::kTrue,
      .lhs = std::make_shared<Operator>(Table{.name = "A"}),
      .rhs = std::make_shared<Operator>(Table{.name = "B"}),
  };

  Memo memo(join_ab, Rules{{}});
}

class Optimizer {
    public:

    explicit Optimizer(const Operator& expr, const Rules& rules) : memo_(expr, rules), rules_count_(rules.Count()) {}

    bool IsExplored(const Group& group) const {
      return explored_groups_.contains(group.GetKey());
    }

    void SetExplored(const Group& group) {
      explored_groups_.insert(group.GetKey());
    }

    void OptimizeInputs(const ExpressionRulesApplier& expr) {
    }

    void ApplyRule(ExpressionRulesApplier& expr, RuleNumber rule) {
      auto new_expr = expr.ApplyRule(rule, memo_);
      auto group = memo_.GetGroup(new_expr.GetGroup());
      auto new_expr_ref = group.AddExpression(std::move(new_expr));
      if (new_expr.IsTransformationRule(rule)) {
        tasks_.emplace([this, &new_expr_ref]() { ExploreExpression(new_expr_ref); });
      } else {
        tasks_.emplace([this, &new_expr_ref]() { OptimizeInputs(new_expr_ref); });
      }
    }

    void OptimizeExpression(ExpressionRulesApplier& expr) {
      for (RuleNumber rule = 0; rule < rules_count_; rule++) {
        if (!expr.IsApplicable(rule, memo_)) {
          continue;
        }
        tasks_.emplace([this, &expr, rule]() {
            ApplyRule(expr, rule);
        });
      }

      for (auto child : expr.GetChildren()) {
        auto& group = memo_.GetGroup(child);
        if (IsExplored(group)) {
          continue;
        }
        tasks_.emplace([this, &group]() {
          ExploreGroup(group);
        });
      }
    }

    void ExploreExpression(ExpressionRulesApplier& expr) {
      for (RuleNumber rule = 0; rule < rules_count_; rule++) {
        bool transformation_rules_only = true;
        if (!expr.IsApplicable(rule, memo_, transformation_rules_only)) {
          continue;
        }
        tasks_.emplace([this, &expr, rule]() {
            ApplyRule(expr, rule);
        });
      }

      for (auto child : expr.GetChildren()) {
        auto& group = memo_.GetGroup(child);
        if (IsExplored(group)) {
          continue;
        }
        tasks_.emplace([this, &group]() {
          ExploreGroup(group);
        });
      }
    }

    void ExploreGroup(Group& group) {
      SetExplored(group);
      for (auto& expr : group.GetExpressions()) {
        tasks_.emplace([this, &expr]() {
          ExploreExpression(expr);
        });
      }
    }

    void OptimizeGroup(Group& group) {
      if (!IsExplored(group)) {
        tasks_.emplace([this, &group](){
          OptimizeGroup(group);
        });
        tasks_.emplace([this, &group](){
          ExploreGroup(group);
        });
        return;
      }

      for (auto& expr : group.GetExpressions()) {
        tasks_.emplace([this, &expr](){
          OptimizeExpression(expr);
        });
      }
    }

    Operator Optimize() {
      tasks_.emplace([this]() {
        OptimizeGroup(memo_.GetRoot());
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
        std::stack<std::function<void()>> tasks_;
        std::unordered_set<GroupKey> explored_groups_;
        const size_t rules_count_;
};

class JoinCommutativity : public Rule {
  public:
    bool IsApplicable(LogicalExpr expr, Memo&) override {
      return std::holds_alternative<logical::Join>(expr);
    }

    bool IsTransformationRule() const override { return true; }

    LogicalExpr Apply(const LogicalExpr& expr, Memo&) override {
      auto join = std::get<logical::Join>(expr);
      std::swap(join.lhs, join.rhs);
      return join;
    }
};

class JoinAssociativity : public Rule {
  public:
    bool IsApplicable(LogicalExpr expr, Memo& memo) override {
      if (!std::holds_alternative<logical::Join>(expr)) return false;
      const auto& outer = std::get<logical::Join>(expr);
      for (auto& applier : memo.GetGroup(outer.lhs).GetExpressions()) {
        if (!std::holds_alternative<logical::Join>(applier.GetExpr())) return false;
      }
      return true;
    }

    bool IsTransformationRule() const override { return true; }

    LogicalExpr Apply(const LogicalExpr& expr, Memo& memo) override {
      const auto& outer = std::get<logical::Join>(expr);
      for (auto& applier : memo.GetGroup(outer.lhs).GetExpressions()) {
        const auto& inner = std::get<logical::Join>(applier.GetExpr());
        auto combined_qual = Expression{BinaryExpression{
            std::make_shared<Expression>(inner.qual),
            BinaryOp::kAnd,
            std::make_shared<Expression>(outer.qual),
        }};
        auto new_rhs = memo.AddGroup(logical::Join{inner.rhs, outer.rhs, outer.type, Literal::kTrue});
        return logical::Join{inner.lhs, new_rhs, inner.type, combined_qual};
      }
      return expr;
    }
};


TEST(OptimizerTest, Simple) {
  std::stringstream s{"SELECT * FROM users;"};
  Operator op = GetAST(s).value();
  Optimizer optimizer(op, Rules{{}});

  auto got = optimizer.Optimize();
}

}  // namespace stewkk::sql
