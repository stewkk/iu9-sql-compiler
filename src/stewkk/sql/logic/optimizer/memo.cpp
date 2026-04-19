#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

std::string ToKey(const LogicalOperator& op) {
    return std::visit(utils::Overloaded{
        [](const logical::Table& t) {
            return "Table(" + t.name + ")";
        },
        [](const logical::Filter& f) {
            return "Filter(" + ToString(f.predicate) + "," + std::to_string(f.source->id()) + ")";
        },
        [](const logical::Projection& p) {
            std::string exprs;
            for (const auto& e : p.expressions) {
                exprs += ToString(e) + ",";
            }
            return "Projection(" + exprs + std::to_string(p.source->id()) + ")";
        },
        [](const logical::CrossJoin& j) {
            return "CrossJoin(" + std::to_string(j.lhs->id()) + "," + std::to_string(j.rhs->id()) + ")";
        },
        [](const logical::Join& j) {
            return "Join(" + ToString(j.type) + "," + ToString(j.qual) + "," +
                   std::to_string(j.lhs->id()) + "," + std::to_string(j.rhs->id()) + ")";
        },
    }, op);
}

}  // namespace

size_t Memo::GroupCount() const {
    return groups_.size();
}

utils::NotNull<Group*> Memo::AddGroup(LogicalOperator root_operator) {
    auto key = ToKey(root_operator);
    auto it = expr_index_.find(key);
    if (it != expr_index_.end()) {
        return it->second;
    }
    auto& ptr = groups_.emplace_back(new Group(groups_.size()));
    ptr->AddLogicalExpr(std::move(root_operator));
    expr_index_[key] = ptr.get();
    return ptr.get();
}

}  // namespace stewkk::sql
