#pragma once

#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <stewkk/sql/logic/optimizer/group.hpp>

namespace stewkk::sql {

class Memo {
  public:
    struct LogicalProvenance {
        size_t rule_id;
        std::string_view rule_name;
        LogicalExpr* source;
    };

    class ScopedLogicalProvenance {
      public:
        ScopedLogicalProvenance(Memo& memo, LogicalProvenance provenance);
        ~ScopedLogicalProvenance();

        ScopedLogicalProvenance(const ScopedLogicalProvenance&) = delete;
        ScopedLogicalProvenance& operator=(const ScopedLogicalProvenance&) = delete;

      private:
        Memo& memo_;
        std::optional<LogicalProvenance> previous_;
    };

    size_t GroupCount() const;
    utils::NotNull<LogicalExpr*> AddGroup(LogicalOperator root_operator);
    LogicalExpr* GetGroup(LogicalOperator root_operator) const;
    utils::NotNull<LogicalExpr*> AddLogicalExprToGroup(utils::NotNull<Group*> group,
                                                       LogicalOperator root_operator);
    utils::NotNull<LogicalExpr*> Populate(const Operator& op);

  private:
    LogicalExpr* GetGroup(const std::string& key) const;
    void SetProvenanceIfNew(utils::NotNull<LogicalExpr*> expr);

    std::deque<Group> groups_;
    std::unordered_map<std::string, LogicalExpr*> expr_index_;
    std::optional<LogicalProvenance> current_provenance_;
};

}  // namespace stewkk::sql
