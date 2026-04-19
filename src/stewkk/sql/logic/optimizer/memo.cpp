#include <stewkk/sql/logic/optimizer/memo.hpp>

namespace stewkk::sql {

size_t Memo::GroupCount() const {
    return groups_.size();
}

utils::NotNull<Group*> Memo::AddGroup(LogicalOperator root_operator) {
    auto& ptr = groups_.emplace_back(new Group());
    ptr->AddLogicalExpr(std::move(root_operator));
    return ptr.get();
}

}  // namespace stewkk::sql
