#pragma once

#include <vector>
#include <memory>

#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

class Memo {
  public:
    size_t GroupCount() const;
    utils::NotNull<Group*> AddGroup(LogicalOperator root_operator);

  private:
    std::vector<std::unique_ptr<Group>> groups_;
};

}  // namespace stewkk::sql
