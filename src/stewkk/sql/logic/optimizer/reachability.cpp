#include <stewkk/sql/logic/optimizer/reachability.hpp>

#include <format>

#include <stewkk/sql/utils/overloaded.hpp>
#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/optimizer/rules.hpp>

namespace stewkk::sql {

namespace {

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
            const auto* t = std::get_if<SeqScan>(&target);
            if (!t) return {false, depth, "type mismatch: expected SeqScan"};
            if (op.table != t->table)
                return {false, depth,
                        std::format("SeqScan table '{}' != '{}'", op.table, t->table)};
            return {true, depth + 1, {}};
        },
        [&](const physical::Filter& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalFilter>(&target);
            if (!t) return {false, depth, "type mismatch: expected Filter"};
            if (op.predicate != t->predicate)
                return {false, depth,
                        std::format("Filter predicate '{}' != '{}'",
                                    ToString(op.predicate), ToString(t->predicate))};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Filter.source: " + child.reason;
            return child;
        },
        [&](const physical::Projection& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalProjection>(&target);
            if (!t) return {false, depth, "type mismatch: expected Projection"};
            if (op.expressions != t->expressions)
                return {false, depth, "Projection expressions mismatch"};
            auto child = MatchGroup(op.source.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Projection.source: " + child.reason;
            return child;
        },
        [&](const physical::NestedLoopJoin& op) -> InternalMatch {
            const auto* t = std::get_if<NestedLoopJoin>(&target);
            if (!t) return {false, depth, "type mismatch: expected NestedLoopJoin"};
            if (op.type != t->type)
                return {false, depth, "NestedLoopJoin join type mismatch"};
            if (op.qual != t->qual)
                return {false, depth,
                        std::format("NestedLoopJoin qual '{}' != '{}'",
                                    ToString(op.qual), ToString(t->qual))};
            auto lhs = MatchGroup(op.lhs.get(), *t->lhs, depth + 1);
            if (!lhs.ok) { lhs.reason = "NestedLoopJoin.lhs: " + lhs.reason; return lhs; }
            auto rhs = MatchGroup(op.rhs.get(), *t->rhs, depth + 1);
            if (!rhs.ok) { rhs.reason = "NestedLoopJoin.rhs: " + rhs.reason; return rhs; }
            return {true, std::max(lhs.depth, rhs.depth), {}};
        },
        [&](const physical::NestedLoopCrossJoin& op) -> InternalMatch {
            const auto* t = std::get_if<NestedLoopCrossJoin>(&target);
            if (!t) return {false, depth, "type mismatch: expected NestedLoopCrossJoin"};
            auto lhs = MatchGroup(op.lhs.get(), *t->lhs, depth + 1);
            if (!lhs.ok) { lhs.reason = "NestedLoopCrossJoin.lhs: " + lhs.reason; return lhs; }
            auto rhs = MatchGroup(op.rhs.get(), *t->rhs, depth + 1);
            if (!rhs.ok) { rhs.reason = "NestedLoopCrossJoin.rhs: " + rhs.reason; return rhs; }
            return {true, std::max(lhs.depth, rhs.depth), {}};
        },
        [&](const physical::Sort& op) -> InternalMatch {
            const auto* t = std::get_if<PhysicalSort>(&target);
            if (!t) return {false, depth, "type mismatch: expected Sort"};
            if (op.keys != t->keys)
                return {false, depth, "Sort keys mismatch"};
            auto child = MatchGroup(op.input.get(), *t->source, depth + 1);
            if (!child.ok) child.reason = "Sort.input: " + child.reason;
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


}  // namespace

MatchResult IsReachable(utils::NotNull<Group*> root, const PhysicalPlanNode& target) {
    auto r = MatchGroup(root.get(), target, 0);
    return {r.ok, r.reason};
}

MatchResult IsPlanReachable(std::istream& sql, const PhysicalPlanNode& target,
                             CardinalityEstimates cardinality) {
    auto ast = GetAST(sql).value();
    Optimizer optimizer(ast, MakeMainRules(), std::move(cardinality));
    optimizer.OptimizeExhaustive();
    return IsReachable(optimizer.GetRootGroup(), target);
}

}  // namespace stewkk::sql
