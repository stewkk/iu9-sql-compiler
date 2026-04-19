#pragma once

#include <vector>
#include <memory>

#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

class Memo {
  public:
    size_t GroupCount() const { return groups_.size(); }

    utils::NotNull<Group*> AddGroup(LogicalOperator root_operator) {
        auto& ptr = groups_.emplace_back(new Group());
        ptr->AddLogicalExpr(std::move(root_operator));
        return ptr.get();
    }

  private:
    std::vector<std::unique_ptr<Group>> groups_;
};

}  // namespace stewkk::sql
