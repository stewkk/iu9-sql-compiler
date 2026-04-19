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

utils::NotNull<LogicalExpr*> Memo::AddGroup(LogicalOperator root_operator) {
    auto key = ToKey(root_operator);
    auto it = expr_index_.find(key);
    if (it != expr_index_.end()) {
        return it->second->GetLogicalExprs()[0].get();
    }
    auto& ptr = groups_.emplace_back(new Group(groups_.size()));
    auto expr = ptr->AddLogicalExpr(std::move(root_operator));
    expr_index_[key] = ptr.get();
    return expr;
}

utils::NotNull<LogicalExpr*> Memo::Populate(const Operator& op) {
    return std::visit(utils::Overloaded{
        [this](const Table& t) {
            return AddGroup(logical::Table{t.name});
        },
        [this](const Filter& f) {
            auto source = Populate(*f.source);
            return AddGroup(logical::Filter{source->group, f.expr});
        },
        [this](const Projection& p) {
            auto source = Populate(*p.source);
            return AddGroup(logical::Projection{source->group, p.expressions});
        },
        [this](const CrossJoin& j) {
            auto lhs = Populate(*j.lhs);
            auto rhs = Populate(*j.rhs);
            return AddGroup(logical::CrossJoin{lhs->group, rhs->group});
        },
        [this](const Join& j) {
            auto lhs = Populate(*j.lhs);
            auto rhs = Populate(*j.rhs);
            return AddGroup(logical::Join{lhs->group, rhs->group, j.type, j.qual});
        },
    }, op);
}

}  // namespace stewkk::sql
