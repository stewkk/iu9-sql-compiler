#include <stewkk/sql/logic/implementation_rules/implement_hash_join.hpp>

namespace stewkk::sql {

namespace {

bool IsSimpleEquiJoin(const Expression& qual) {
    const auto* bin = std::get_if<BinaryExpression>(&qual);
    if (!bin || bin->binop != BinaryOp::kEq) return false;
    return std::holds_alternative<Attribute>(*bin->lhs)
        && std::holds_alternative<Attribute>(*bin->rhs);
}

} // namespace

bool ImplementHashJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
    const auto& join = std::get<logical::Join>(expr->root_operator);
    if (join.type != JoinType::kInner) return false;
    return IsSimpleEquiJoin(join.qual);
}

std::vector<utils::NotNull<PhysicalExpr*>> ImplementHashJoin::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& join = std::get<logical::Join>(expr->root_operator);
    return {expr->group->AddPhysicalExpr(
        physical::HashJoin{join.lhs, join.rhs, join.type, join.qual})};
}

}  // namespace stewkk::sql
