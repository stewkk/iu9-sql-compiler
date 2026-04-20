#pragma once

#include <deque>
#include <ranges>

#include <stewkk/sql/logic/optimizer/logical_expr.hpp>
#include <stewkk/sql/logic/optimizer/physical_expr.hpp>

namespace stewkk::sql {

using LogicalOperator = decltype(LogicalExpr::root_operator);

class Group {
  private:
    struct ToNotNull {
        utils::NotNull<LogicalExpr*> operator()(LogicalExpr& e) const { return &e; }
    };

  public:
    using LogicalExprs = std::ranges::transform_view<
        std::ranges::ref_view<std::deque<LogicalExpr>>,
        ToNotNull>;

    utils::NotNull<LogicalExpr*> AddLogicalExpr(LogicalOperator root_operator);
    utils::NotNull<PhysicalExpr*> AddPhysicalExpr();
    LogicalExprs GetLogicalExprs();

    size_t GetId() const;

  private:
    friend class Memo;
    explicit Group(size_t id) : id_(id) {}

    size_t id_;
    std::deque<LogicalExpr>  logical_exprs_;
    std::deque<PhysicalExpr> physical_exprs_;
};

}  // namespace stewkk::sql
