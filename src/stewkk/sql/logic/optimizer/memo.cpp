#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

std::string ToKey(const LogicalOperator& op) {
    return std::visit(utils::Overloaded{
        [](const logical::Table& t) {
            return "Table(" + t.name + "," + std::string{VisibleName(t)} + ")";
        },
        [](const logical::Filter& f) {
            return "Filter(" + ToString(f.predicate) + "," + std::to_string(f.source->GetId()) + ")";
        },
        [](const logical::Projection& p) {
            std::string exprs;
            for (const auto& e : p.expressions) {
                exprs += ToString(e) + ",";
            }
            std::string aliases;
            for (const auto& alias : p.aliases) {
                aliases += alias.value_or("") + ",";
            }
            return "Projection(" + exprs + aliases + std::to_string(p.source->GetId()) + ")";
        },
        [](const logical::Aggregation& a) {
            std::string group_by;
            for (const auto& e : a.group_by) {
                group_by += ToString(e) + ",";
            }
            std::string aggregates;
            for (const auto& e : a.aggregates) {
                aggregates += ToString(e) + ",";
            }
            return "Aggregation(" + group_by + ";" + aggregates + ";"
                   + std::to_string(a.source->GetId()) + ")";
        },
        [](const logical::CrossJoin& j) {
            return "CrossJoin(" + std::to_string(j.lhs->GetId()) + "," + std::to_string(j.rhs->GetId()) + ")";
        },
        [](const logical::Join& j) {
            return "Join(" + ToString(j.type) + "," + ToString(j.qual) + "," +
                   std::to_string(j.lhs->GetId()) + "," + std::to_string(j.rhs->GetId()) + ")";
        },
    }, op);
}

}  // namespace

size_t Memo::GroupCount() const {
    return groups_.size();
}

LogicalExpr* Memo::GetGroup(LogicalOperator root_operator) const {
    auto key = ToKey(root_operator);
    return GetGroup(key);
}

LogicalExpr* Memo::GetGroup(const std::string& key) const {
    auto it = expr_index_.find(key);
    if (it != expr_index_.end()) {
        return it->second;
    }
    return nullptr;
}

utils::NotNull<LogicalExpr*> Memo::AddGroup(LogicalOperator root_operator) {
    auto key = ToKey(root_operator);
    if (auto g = GetGroup(key); g) {
        return g;
    }
    auto& group = groups_.emplace_back(Group(groups_.size()));
    auto expr = group.AddLogicalExpr(std::move(root_operator));
    expr_index_[key] = expr;
    return expr;
}

utils::NotNull<LogicalExpr*> Memo::AddLogicalExprToGroup(utils::NotNull<Group*> group,
                                                         LogicalOperator root_operator) {
    auto key = ToKey(root_operator);
    if (auto g = GetGroup(key); g) {
        return g;
    }
    auto expr = group->AddLogicalExpr(std::move(root_operator));
    expr_index_[key] = expr;
    return expr;
}

utils::NotNull<LogicalExpr*> Memo::Populate(const Operator& op) {
    return std::visit(utils::Overloaded{
        [this](const Table& t) {
            return AddGroup(logical::Table{t.name, t.alias});
        },
        [this](const Filter& f) {
            auto source = Populate(*f.source);
            return AddGroup(logical::Filter{source->group, f.expr});
        },
        [this](const Projection& p) {
            auto source = Populate(*p.source);
            return AddGroup(logical::Projection{source->group, p.expressions, p.aliases});
        },
        [this](const Aggregation& a) {
            auto source = Populate(*a.source);
            return AddGroup(logical::Aggregation{source->group, a.group_by, a.aggregates});
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
