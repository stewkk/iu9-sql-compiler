#include <stewkk/sql/logic/implementation_rules/implement_merge_join.hpp>

namespace stewkk::sql {

namespace {

bool IsSimpleEquiJoin(const Expression& qual) {
    const auto* bin = std::get_if<BinaryExpression>(&qual);
    if (!bin || bin->binop != BinaryOp::kEq) return false;
    return std::holds_alternative<Attribute>(*bin->lhs)
        && std::holds_alternative<Attribute>(*bin->rhs);
}

}  // namespace

bool ImplementMergeJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
    if (!std::holds_alternative<logical::Join>(expr->root_operator)) return false;
    const auto& join = std::get<logical::Join>(expr->root_operator);
    return IsSimpleEquiJoin(join.qual);
}

std::vector<utils::NotNull<PhysicalExpr*>> ImplementMergeJoin::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
    auto& join = std::get<logical::Join>(expr->root_operator);
    return {expr->group->AddPhysicalExpr(
        physical::MergeJoin{join.lhs, join.rhs, join.type, join.qual})};
}

}  // namespace stewkk::sql
