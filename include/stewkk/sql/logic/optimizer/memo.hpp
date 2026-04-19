#pragma once

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>

#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

class Memo {
  public:
    size_t GroupCount() const;
    utils::NotNull<LogicalExpr*> AddGroup(LogicalOperator root_operator);
    utils::NotNull<LogicalExpr*> Populate(const Operator& op);

  private:
    std::vector<std::unique_ptr<Group>> groups_;
    std::unordered_map<std::string, Group*> expr_index_;
};

}  // namespace stewkk::sql
