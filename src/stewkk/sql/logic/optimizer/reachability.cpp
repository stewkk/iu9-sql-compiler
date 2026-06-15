#include <stewkk/sql/logic/optimizer/reachability.hpp>

#include <algorithm>
#include <format>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <stewkk/sql/utils/overloaded.hpp>
#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_property.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>
#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>
#include <stewkk/sql/utils/output_dot_plans.hpp>

namespace stewkk::sql {

namespace {

constexpr int kInfiniteDistance = std::numeric_limits<int>::max() / 4;

struct InternalMatch {
    bool ok;
    int depth;
    std::string reason;
};

InternalMatch MatchGroup(Group* group, const PhysicalPlanNode& target, int depth);

InternalMatch TryMatchExpr(utils::NotNull<PhysicalExpr*> pe,
                                  const PhysicalPlanNode& target, int depth) {
    return std::visit(utils::Overloaded{
        [&](const physical::SeqScan& op) -> InternalMatch {
            const auto* t = std::get_if<SeqScan>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected SeqScan"};
            if (op.table != t->table)
                return {false, depth + 1,
                        std::format("SeqScan table '{}' != '{}'", op.table, t->table)};
            if (op.alias != t->alias)
                return {false, depth + 1,
                        std::format("SeqScan alias '{}' != '{}'",
                                    op.alias.value_or(""), t->alias.value_or(""))};
            return {true, depth + 1, {}};
        },
        [&](const physical::IndexSeek& op) -> InternalMatch {
            const auto* t = std::get_if<IndexSeek>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected IndexSeek"};
            if (op.table != t->table)
                return {false, depth + 1,
                        std::format("IndexSeek table '{}' != '{}'", op.table, t->table)};
            if (op.alias != t->alias)
                return {false, depth + 1,
                        std::format("IndexSeek alias '{}' != '{}'",
                                    op.alias.value_or(""), t->alias.value_or(""))};
            if (!EquivalentPredicate(op.predicate, t->predicate))
                return {false, depth + 1,
                        std::format("IndexSeek predicate '{}' != '{}'",
                                    ToString(op.predicate), ToString(t->predicate))};
            return {true, depth + 1, {}};
        },
        [&](const physical::Filter& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalFilter>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected Filter"};
            if (!EquivalentPredicate(op.predicate, t->predicate))
                return {false, depth + 1,
                        std::format("Filter predicate '{}' != '{}'",
                                    ToString(op.predicate), ToString(t->predicate))};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Filter.source: " + child.reason;
            return child;
        },
        [&](const physical::Projection& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalProjection>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected Projection"};
            if (op.expressions != t->expressions)
                return {false, depth + 1, "Projection expressions mismatch"};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Projection.source: " + child.reason;
            return child;
        },
        [&](const physical::NestedLoopJoin& op) -> InternalMatch {
            const auto* t = std::get_if<NestedLoopJoin>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected NestedLoopJoin"};
            if (op.type != t->type)
                return {false, depth + 1, "NestedLoopJoin join type mismatch"};
            if (!EquivalentPredicate(op.qual, t->qual))
                return {false, depth + 1,
                        std::format("NestedLoopJoin qual '{}' != '{}'",
                                    ToString(op.qual), ToString(t->qual))};
            auto lhs = MatchGroup(op.lhs.get(), *t->lhs, depth + 1);
            if (!lhs.ok) { lhs.reason = "NestedLoopJoin.lhs: " + lhs.reason; return lhs; }
            auto rhs = MatchGroup(op.rhs.get(), *t->rhs, depth + 1);
            if (!rhs.ok) { rhs.reason = "NestedLoopJoin.rhs: " + rhs.reason; return rhs; }
            return {true, std::max(lhs.depth, rhs.depth), {}};
        },
        [&](const physical::NestedLoopCrossJoin& op) -> InternalMatch {
            const auto* t = std::get_if<NestedLoopCrossJoin>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected NestedLoopCrossJoin"};
            auto lhs = MatchGroup(op.lhs.get(), *t->lhs, depth + 1);
            if (!lhs.ok) { lhs.reason = "NestedLoopCrossJoin.lhs: " + lhs.reason; return lhs; }
            auto rhs = MatchGroup(op.rhs.get(), *t->rhs, depth + 1);
            if (!rhs.ok) { rhs.reason = "NestedLoopCrossJoin.rhs: " + rhs.reason; return rhs; }
            return {true, std::max(lhs.depth, rhs.depth), {}};
        },
        [&](const physical::HashJoin& op) -> InternalMatch {
            const auto* t = std::get_if<HashJoin>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected HashJoin"};
            if (op.type != t->type)
                return {false, depth + 1, "HashJoin join type mismatch"};
            if (!EquivalentPredicate(op.qual, t->qual))
                return {false, depth + 1,
                        std::format("HashJoin qual '{}' != '{}'",
                                    ToString(op.qual), ToString(t->qual))};
            auto lhs = MatchGroup(op.lhs.get(), *t->lhs, depth + 1);
            if (!lhs.ok) { lhs.reason = "HashJoin.lhs: " + lhs.reason; return lhs; }
            auto rhs = MatchGroup(op.rhs.get(), *t->rhs, depth + 1);
            if (!rhs.ok) { rhs.reason = "HashJoin.rhs: " + rhs.reason; return rhs; }
            return {true, std::max(lhs.depth, rhs.depth), {}};
        },
        [&](const physical::MergeJoin& op) -> InternalMatch {
            const auto* t = std::get_if<MergeJoin>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected MergeJoin"};
            if (op.type != t->type)
                return {false, depth + 1, "MergeJoin join type mismatch"};
            if (!EquivalentPredicate(op.qual, t->qual))
                return {false, depth + 1,
                        std::format("MergeJoin qual '{}' != '{}'",
                                    ToString(op.qual), ToString(t->qual))};
            auto lhs = MatchGroup(op.lhs.get(), *t->lhs, depth + 1);
            if (!lhs.ok) { lhs.reason = "MergeJoin.lhs: " + lhs.reason; return lhs; }
            auto rhs = MatchGroup(op.rhs.get(), *t->rhs, depth + 1);
            if (!rhs.ok) { rhs.reason = "MergeJoin.rhs: " + rhs.reason; return rhs; }
            return {true, std::max(lhs.depth, rhs.depth), {}};
        },
        [&](const physical::Sort& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalSort>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected Sort"};
            if (op.keys != t->keys)
                return {false, depth + 1, "Sort keys mismatch"};
            auto child = MatchGroup(op.input.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Sort.input: " + child.reason;
            return child;
        },
        [&](const physical::Aggregation& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalAggregation>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected HashAggregate"};
            if (op.group_by != t->group_by)
                return {false, depth + 1, "Aggregation group_by mismatch"};
            if (op.aggregates != t->aggregates)
                return {false, depth + 1, "Aggregation aggregates mismatch"};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Aggregation.source: " + child.reason;
            return child;
        },
        [&](const physical::StreamAggregation& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalStreamAggregation>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected StreamAggregate"};
            if (op.group_by != t->group_by)
                return {false, depth + 1, "StreamAggregation group_by mismatch"};
            if (op.aggregates != t->aggregates)
                return {false, depth + 1, "StreamAggregation aggregates mismatch"};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "StreamAggregation.source: " + child.reason;
            return child;
        },
        [&](const physical::PartialAggregation& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalPartialAggregation>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected PartialAggregate"};
            if (op.group_by != t->group_by)
                return {false, depth + 1, "PartialAggregation group_by mismatch"};
            if (op.aggregates != t->aggregates)
                return {false, depth + 1, "PartialAggregation aggregates mismatch"};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "PartialAggregation.source: " + child.reason;
            return child;
        },
        [&](const physical::FinalAggregation& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalFinalAggregation>(&target.node);
            if (!t) return {false, depth, "type mismatch: expected FinalAggregate"};
            if (op.group_by != t->group_by)
                return {false, depth + 1, "FinalAggregation group_by mismatch"};
            if (op.aggregates != t->aggregates)
                return {false, depth + 1, "FinalAggregation aggregates mismatch"};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "FinalAggregation.source: " + child.reason;
            return child;
        },
    }, pe->root_operator);
}

InternalMatch MatchGroup(Group* group, const PhysicalPlanNode& target, int depth) {
    InternalMatch best{false, -1,
                       std::format("group {}: no physical expressions generated", group->GetId())};
    for (auto pe : group->GetPhysicalExprs()) {
        auto r = TryMatchExpr(pe, target, depth);
        if (r.ok) return r;
        if (r.depth > best.depth) best = r;
    }
    return best;
}

std::vector<Group*> Children(const PhysicalExpr& expr) {
    return std::visit(utils::Overloaded{
        [](const physical::SeqScan&) { return std::vector<Group*>{}; },
        [](const physical::IndexSeek&) { return std::vector<Group*>{}; },
        [](const physical::Filter& op) { return std::vector<Group*>{op.source.get()}; },
        [](const physical::Projection& op) { return std::vector<Group*>{op.source.get()}; },
        [](const physical::NestedLoopJoin& op) {
            return std::vector<Group*>{op.lhs.get(), op.rhs.get()};
        },
        [](const physical::NestedLoopCrossJoin& op) {
            return std::vector<Group*>{op.lhs.get(), op.rhs.get()};
        },
        [](const physical::HashJoin& op) {
            return std::vector<Group*>{op.lhs.get(), op.rhs.get()};
        },
        [](const physical::MergeJoin& op) {
            return std::vector<Group*>{op.lhs.get(), op.rhs.get()};
        },
        [](const physical::Sort& op) { return std::vector<Group*>{op.input.get()}; },
        [](const physical::Aggregation& op) { return std::vector<Group*>{op.source.get()}; },
        [](const physical::StreamAggregation& op) { return std::vector<Group*>{op.source.get()}; },
        [](const physical::PartialAggregation& op) { return std::vector<Group*>{op.source.get()}; },
        [](const physical::FinalAggregation& op) { return std::vector<Group*>{op.source.get()}; },
    }, expr.root_operator);
}

std::vector<const PhysicalPlanNode*> Children(const PhysicalPlanNode& node) {
    return std::visit(utils::Overloaded{
        [](const SeqScan&) { return std::vector<const PhysicalPlanNode*>{}; },
        [](const IndexSeek&) { return std::vector<const PhysicalPlanNode*>{}; },
        [](const PhysicalFilter& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
        [](const PhysicalProjection& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
        [](const NestedLoopJoin& op) {
            return std::vector<const PhysicalPlanNode*>{op.lhs.get(), op.rhs.get()};
        },
        [](const NestedLoopCrossJoin& op) {
            return std::vector<const PhysicalPlanNode*>{op.lhs.get(), op.rhs.get()};
        },
        [](const HashJoin& op) {
            return std::vector<const PhysicalPlanNode*>{op.lhs.get(), op.rhs.get()};
        },
        [](const MergeJoin& op) {
            return std::vector<const PhysicalPlanNode*>{op.lhs.get(), op.rhs.get()};
        },
        [](const PhysicalSort& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
        [](const PhysicalAggregation& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
        [](const PhysicalStreamAggregation& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
        [](const PhysicalPartialAggregation& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
        [](const PhysicalFinalAggregation& op) { return std::vector<const PhysicalPlanNode*>{op.source.get()}; },
    }, node.node);
}

int TargetTreeSize(const PhysicalPlanNode& node) {
    int size = 1;
    for (const auto* child : Children(node)) {
        size += TargetTreeSize(*child);
    }
    return size;
}

struct PairHash {
    size_t operator()(const std::pair<Group*, const PhysicalPlanNode*>& p) const noexcept {
        size_t h = std::hash<Group*>{}(p.first);
        h ^= std::hash<const PhysicalPlanNode*>{}(p.second) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

class TreeDistance {
  public:
    int Distance(Group* group, const PhysicalPlanNode& target) {
        auto key = std::pair{group, &target};
        if (auto it = distance_memo_.find(key); it != distance_memo_.end()) {
            return it->second;
        }
        if (!distance_visiting_.insert(key).second) {
            return kInfiniteDistance;
        }

        int best = kInfiniteDistance;
        for (auto pe : group->GetPhysicalExprs()) {
            best = std::min(best, Distance(*pe, target));
        }
        distance_visiting_.erase(key);
        distance_memo_.emplace(key, best);
        return best;
    }

  private:
    int MinTreeSize(Group* group) {
        if (auto it = size_memo_.find(group); it != size_memo_.end()) {
            return it->second;
        }
        if (!size_visiting_.insert(group).second) {
            return kInfiniteDistance;
        }

        int best = kInfiniteDistance;
        for (auto pe : group->GetPhysicalExprs()) {
            int size = 1;
            for (auto* child : Children(*pe)) {
                int child_size = MinTreeSize(child);
                if (child_size >= kInfiniteDistance) {
                    size = kInfiniteDistance;
                    break;
                }
                size += child_size;
            }
            best = std::min(best, size);
        }

        size_visiting_.erase(group);
        size_memo_.emplace(group, best);
        return best;
    }

    int Distance(const PhysicalExpr& expr, const PhysicalPlanNode& target) {
        const auto source_children = Children(expr);
        const auto target_children = Children(target);
        return LabelCost(expr, target) + ChildrenDistance(source_children, target_children);
    }

    int ChildrenDistance(const std::vector<Group*>& source_children,
                         const std::vector<const PhysicalPlanNode*>& target_children) {
        std::vector<std::vector<int>> dp(
            source_children.size() + 1, std::vector<int>(target_children.size() + 1, 0));

        for (size_t i = 1; i <= source_children.size(); ++i) {
            dp[i][0] = dp[i - 1][0] + MinTreeSize(source_children[i - 1]);
        }
        for (size_t j = 1; j <= target_children.size(); ++j) {
            dp[0][j] = dp[0][j - 1] + TargetTreeSize(*target_children[j - 1]);
        }

        for (size_t i = 1; i <= source_children.size(); ++i) {
            for (size_t j = 1; j <= target_children.size(); ++j) {
                const int del = dp[i - 1][j] + MinTreeSize(source_children[i - 1]);
                const int ins = dp[i][j - 1] + TargetTreeSize(*target_children[j - 1]);
                const int sub = dp[i - 1][j - 1]
                                + Distance(source_children[i - 1], *target_children[j - 1]);
                dp[i][j] = std::min({del, ins, sub});
            }
        }
        return dp[source_children.size()][target_children.size()];
    }

    static int LabelCost(const PhysicalExpr& expr, const PhysicalPlanNode& target) {
        return std::visit(utils::Overloaded{
            [](const physical::SeqScan& op, const SeqScan& t) {
                return op.table == t.table && op.alias == t.alias ? 0 : 1;
            },
            [](const physical::IndexSeek& op, const IndexSeek& t) {
                return op.table == t.table && op.alias == t.alias
                       && EquivalentPredicate(op.predicate, t.predicate)
                    ? 0
                    : 1;
            },
            [](const physical::Filter& op, const PhysicalFilter& t) {
                return EquivalentPredicate(op.predicate, t.predicate) ? 0 : 1;
            },
            [](const physical::Projection& op, const PhysicalProjection& t) {
                return op.expressions == t.expressions ? 0 : 1;
            },
            [](const physical::NestedLoopJoin& op, const NestedLoopJoin& t) {
                return op.type == t.type && EquivalentPredicate(op.qual, t.qual) ? 0 : 1;
            },
            [](const physical::NestedLoopCrossJoin&, const NestedLoopCrossJoin&) {
                return 0;
            },
            [](const physical::HashJoin& op, const HashJoin& t) {
                return op.type == t.type && EquivalentPredicate(op.qual, t.qual) ? 0 : 1;
            },
            [](const physical::MergeJoin& op, const MergeJoin& t) {
                return op.type == t.type && EquivalentPredicate(op.qual, t.qual) ? 0 : 1;
            },
            [](const physical::Sort& op, const PhysicalSort& t) {
                return op.keys == t.keys ? 0 : 1;
            },
            [](const physical::Aggregation& op, const PhysicalAggregation& t) {
                return op.group_by == t.group_by && op.aggregates == t.aggregates ? 0 : 1;
            },
            [](const physical::StreamAggregation& op, const PhysicalStreamAggregation& t) {
                return op.group_by == t.group_by && op.aggregates == t.aggregates ? 0 : 1;
            },
            [](const physical::PartialAggregation& op, const PhysicalPartialAggregation& t) {
                return op.group_by == t.group_by && op.aggregates == t.aggregates ? 0 : 1;
            },
            [](const physical::FinalAggregation& op, const PhysicalFinalAggregation& t) {
                return op.group_by == t.group_by && op.aggregates == t.aggregates ? 0 : 1;
            },
            [](const auto&, const auto&) {
                return 1;
            },
        }, expr.root_operator, target.node);
    }

    std::unordered_map<std::pair<Group*, const PhysicalPlanNode*>, int, PairHash> distance_memo_;
    std::unordered_set<std::pair<Group*, const PhysicalPlanNode*>, PairHash> distance_visiting_;
    std::unordered_map<Group*, int> size_memo_;
    std::unordered_set<Group*> size_visiting_;
};


}  // namespace

MatchResult IsReachable(utils::NotNull<Group*> root, const PhysicalPlanNode& target) {
    auto r = MatchGroup(root.get(), target, 0);
    TreeDistance distance;
    return {r.ok, r.reason, distance.Distance(root.get(), target)};
}

MatchResult IsPlanReachable(std::istream& sql, const PhysicalPlanNode& target,
                             CardinalityEstimates cardinality, SchemaCatalog schema,
                             IndexCatalog indexes) {
    auto parsed = GetAST(sql).value();
    PropertySet required = parsed.required_order
        ? PropertySet{SortProperty{*parsed.required_order}}
        : PropertySet::Any();
    Optimizer optimizer(parsed.op, MakeMainRules(std::move(indexes)), std::move(cardinality),
                        std::move(schema), std::move(required));
    auto plan = optimizer.OptimizeExhaustive();
    auto result = IsReachable(optimizer.GetRootGroup(), target);
    if (!result.reachable) {
        OutputDot(plan, target);
    }
    return result;
}

}  // namespace stewkk::sql
