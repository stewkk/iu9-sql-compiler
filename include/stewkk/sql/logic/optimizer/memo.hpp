#pragma once

#include <deque>
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
    std::deque<Group> groups_;
    std::unordered_map<std::string, LogicalExpr*> expr_index_;
};

}  // namespace stewkk::sql
