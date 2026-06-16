#include <stewkk/sql/logic/implementation_rules/implement_hash_join.hpp>

#include <algorithm>
#include <vector>

#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

bool IsSimpleEquiJoin(const Expression& qual) {
    const auto* bin = std::get_if<BinaryExpression>(&qual);
    if (!bin || bin->binop != BinaryOp::kEq) return false;
    return std::holds_alternative<Attribute>(*bin->lhs)
        && std::holds_alternative<Attribute>(*bin->rhs);
}

bool HasEquiJoinConjunct(const Expression& qual) {
    std::vector<Expression> conjuncts;
    CollectConjuncts(qual, conjuncts);
    return std::ranges::any_of(conjuncts, IsSimpleEquiJoin);
}

} // namespace

bool ImplementHashJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
    const auto& join = std::get<logical::Join>(expr->root_operator);
    if (join.type != JoinType::kInner) return false;
    return HasEquiJoinConjunct(join.qual);
}

std::vector<utils::NotNull<PhysicalExpr*>> ImplementHashJoin::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& join = std::get<logical::Join>(expr->root_operator);
    return {expr->group->AddPhysicalExpr(
        physical::HashJoin{join.lhs, join.rhs, join.type, join.qual})};
}

}  // namespace stewkk::sql
