#include <stewkk/sql/logic/transformation_rules/filter_pushdown_through_join.hpp>

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <vector>

#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

struct Sides {
  bool push_left;
  bool push_right;
};

Sides PushableSides(JoinType type) {
  switch (type) {
    case JoinType::kInner: return {true, true};
    case JoinType::kLeft:  return {true, false};
    case JoinType::kRight: return {false, true};
    case JoinType::kFull:  return {false, false};
  }
  return {false, false};
}

struct Partition {
  std::vector<Expression> left;
  std::vector<Expression> right;
  std::vector<Expression> rest;
};

Partition PartitionConjuncts(const std::vector<Expression>& conjs,
                             const std::unordered_set<std::string>& l_tables,
                             const std::unordered_set<std::string>& r_tables,
                             Sides sides) {
  Partition out;
  for (const auto& q : conjs) {
    auto q_tables = ExprTables(q);
    bool fits_left = !q_tables.empty() && std::all_of(q_tables.begin(), q_tables.end(),
        [&](const auto& t) { return l_tables.contains(t); });
    bool fits_right = !q_tables.empty() && std::all_of(q_tables.begin(), q_tables.end(),
        [&](const auto& t) { return r_tables.contains(t); });
    if (fits_left && sides.push_left) {
      out.left.push_back(q);
    } else if (fits_right && sides.push_right) {
      out.right.push_back(q);
    } else {
      out.rest.push_back(q);
    }
  }
  return out;
}

const logical::Join* FindPushableJoin(utils::NotNull<Group*> source,
                                      const Expression& predicate) {
  std::vector<Expression> conjs;
  CollectConjuncts(predicate, conjs);
  if (conjs.empty()) return nullptr;

  for (auto inner_expr : source->GetLogicalExprs()) {
    const auto* j = std::get_if<logical::Join>(&inner_expr->root_operator);
    if (j == nullptr) continue;
    auto sides = PushableSides(j->type);
    if (!sides.push_left && !sides.push_right) continue;

    auto l_tables = GroupTables(j->lhs);
    auto r_tables = GroupTables(j->rhs);
    auto parts = PartitionConjuncts(conjs, l_tables, r_tables, sides);
    if (!parts.left.empty() || !parts.right.empty()) {
      return j;
    }
  }
  return nullptr;
}

}  // namespace

bool FilterPushdownThroughJoin::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) return false;
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  return FindPushableJoin(f.source, f.predicate) != nullptr;
}

LogicalOperator FilterPushdownThroughJoin::ApplyImpl(utils::NotNull<LogicalExpr*> expr,
                                                     Memo& memo) {
  const auto& f = std::get<logical::Filter>(expr->root_operator);
  const auto* join = FindPushableJoin(f.source, f.predicate);
  if (join == nullptr) {
    throw std::runtime_error{"FilterPushdownThroughJoin requires a pushable join below"};
  }

  std::vector<Expression> conjs;
  CollectConjuncts(f.predicate, conjs);
  auto sides = PushableSides(join->type);
  auto l_tables = GroupTables(join->lhs);
  auto r_tables = GroupTables(join->rhs);
  auto parts = PartitionConjuncts(conjs, l_tables, r_tables, sides);

  utils::NotNull<Group*> new_lhs = join->lhs;
  if (!parts.left.empty()) {
    new_lhs = memo.AddGroup(logical::Filter{join->lhs, AndConjuncts(parts.left)})->group;
  }
  utils::NotNull<Group*> new_rhs = join->rhs;
  if (!parts.right.empty()) {
    new_rhs = memo.AddGroup(logical::Filter{join->rhs, AndConjuncts(parts.right)})->group;
  }

  logical::Join new_join{new_lhs, new_rhs, join->type, join->qual};

  if (parts.rest.empty()) {
    return new_join;
  }
  auto new_join_group = memo.AddGroup(new_join)->group;
  return logical::Filter{new_join_group, AndConjuncts(parts.rest)};
}

}  // namespace stewkk::sql
