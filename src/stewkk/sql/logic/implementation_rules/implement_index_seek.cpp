#include <stewkk/sql/logic/implementation_rules/implement_index_seek.hpp>

#include <algorithm>
#include <ranges>
#include <utility>
#include <vector>

#include <stewkk/sql/logic/optimizer/memo.hpp>
#include <stewkk/sql/logic/transformation_rules/predicate_utils.hpp>

namespace stewkk::sql {

namespace {

bool IsSupportedComparison(BinaryOp op) {
  return std::ranges::contains(std::vector{BinaryOp::kEq, BinaryOp::kLt, BinaryOp::kLe,
                                           BinaryOp::kGt, BinaryOp::kGe}, op);
}

const logical::Table* SourceTable(utils::NotNull<Group*> group) {
  auto exprs = group->GetLogicalExprs();
  if (exprs.size() != 1) {
    return nullptr;
  }
  return std::get_if<logical::Table>(&exprs.front()->root_operator);
}

bool AttrMatchesTable(const Attribute& attr, const logical::Table& table) {
  auto visible = std::string_view{VisibleName(table)};
  return attr.table.empty() || attr.table == table.name || attr.table == visible;
}

bool IsIndexedComparison(const Expression& expr, const logical::Table& table,
                         const IndexCatalog& indexes) {
  const auto* binary = std::get_if<BinaryExpression>(&expr);
  if (!binary || !IsSupportedComparison(binary->binop)) {
    return false;
  }

  if (const auto* attr = std::get_if<Attribute>(binary->lhs.get());
      attr && AttrMatchesTable(*attr, table) && std::holds_alternative<IntConst>(*binary->rhs)) {
    return indexes.HasSortedIndex(table.name, attr->name);
  }
  if (const auto* attr = std::get_if<Attribute>(binary->rhs.get());
      attr && AttrMatchesTable(*attr, table) && std::holds_alternative<IntConst>(*binary->lhs)) {
    return indexes.HasSortedIndex(table.name, attr->name);
  }
  return false;
}

void CollectIndexedColumns(const Expression& expr, const logical::Table& table,
                           const IndexCatalog& indexes, std::vector<std::string>& columns) {
  const auto* binary = std::get_if<BinaryExpression>(&expr);
  if (!binary) {
    return;
  }
  if (binary->binop == BinaryOp::kAnd) {
    CollectIndexedColumns(*binary->lhs, table, indexes, columns);
    CollectIndexedColumns(*binary->rhs, table, indexes, columns);
    return;
  }
  if (!IsSupportedComparison(binary->binop)) {
    return;
  }

  auto add_if_indexed = [&](const Attribute& attr, const Expression& other) {
    if (!AttrMatchesTable(attr, table) || !std::holds_alternative<IntConst>(other)) {
      return;
    }
    if (!indexes.HasSortedIndex(table.name, attr.name)) {
      return;
    }
    if (!std::ranges::contains(columns, attr.name)) {
      columns.push_back(attr.name);
    }
  };

  if (const auto* attr = std::get_if<Attribute>(binary->lhs.get())) {
    add_if_indexed(*attr, *binary->rhs);
  }
  if (const auto* attr = std::get_if<Attribute>(binary->rhs.get())) {
    add_if_indexed(*attr, *binary->lhs);
  }
}

}  // namespace

ImplementIndexSeek::ImplementIndexSeek(IndexCatalog indexes)
    : indexes_(std::move(indexes)) {}

bool HasCompatibleIndexSeek(const logical::Filter& filter, const IndexCatalog& indexes) {
  const auto* table = SourceTable(filter.source);
  if (!table) {
    return false;
  }

  std::vector<Expression> conjuncts;
  CollectConjuncts(filter.predicate, conjuncts);
  return std::ranges::any_of(conjuncts, [&](const Expression& conjunct) {
    return IsIndexedComparison(conjunct, *table, indexes);
  });
}

bool ImplementIndexSeek::IsApplicable(utils::NotNull<LogicalExpr*> expr) {
  if (!std::holds_alternative<logical::Filter>(expr->root_operator)) {
    return false;
  }
  return HasCompatibleIndexSeek(std::get<logical::Filter>(expr->root_operator), indexes_);
}

std::vector<utils::NotNull<PhysicalExpr*>> ImplementIndexSeek::Apply(utils::NotNull<LogicalExpr*> expr, Memo&) {
  auto& filter = std::get<logical::Filter>(expr->root_operator);
  const auto* table = SourceTable(filter.source);
  std::vector<std::string> columns;
  CollectIndexedColumns(filter.predicate, *table, indexes_, columns);

  std::vector<utils::NotNull<PhysicalExpr*>> result;
  result.reserve(columns.size());
  for (const auto& column : columns) {
    result.push_back(expr->group->AddPhysicalExpr(physical::IndexSeek{
        .table = table->name,
        .alias = table->alias,
        .predicate = filter.predicate,
        .index_column = column,
    }));
  }
  return result;
}

}  // namespace stewkk::sql
