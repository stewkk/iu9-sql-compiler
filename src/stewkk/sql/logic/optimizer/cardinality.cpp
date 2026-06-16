#include <stewkk/sql/logic/optimizer/cardinality.hpp>

#include <algorithm>

#include <stewkk/sql/utils/overloaded.hpp>

namespace stewkk::sql {

namespace {

constexpr double kEqualitySelectivity = 0.10;
constexpr double kRangeSelectivity = 0.50;
constexpr double kUnknownSelectivity = 0.50;

bool IsConstant(const Expression& expr) {
  return std::holds_alternative<IntConst>(expr)
      || std::holds_alternative<StringConst>(expr)
      || std::holds_alternative<Literal>(expr);
}

double EstimateFilterSelectivity(const Expression& predicate) {
  return std::visit(utils::Overloaded{
      [](const BinaryExpression& b) {
        const auto lhs = EstimateFilterSelectivity(*b.lhs);
        const auto rhs = EstimateFilterSelectivity(*b.rhs);
        switch (b.binop) {
          case BinaryOp::kAnd:
            return lhs * rhs;
          case BinaryOp::kOr:
            return lhs + rhs - lhs * rhs;
          case BinaryOp::kEq:
            return (std::holds_alternative<Attribute>(*b.lhs) && IsConstant(*b.rhs))
                || (IsConstant(*b.lhs) && std::holds_alternative<Attribute>(*b.rhs))
                ? kEqualitySelectivity
                : kUnknownSelectivity;
          case BinaryOp::kGt:
          case BinaryOp::kLt:
          case BinaryOp::kLe:
          case BinaryOp::kGe:
            return kRangeSelectivity;
          default:
            return kUnknownSelectivity;
        }
      },
      [](const UnaryExpression& u) {
        return u.op == UnaryOp::kNot
            ? 1.0 - EstimateFilterSelectivity(*u.child)
            : kUnknownSelectivity;
      },
      [](const InExpression& in) {
        const auto selectivity = std::min(0.50, in.values.size() * kEqualitySelectivity);
        return in.negated ? 1.0 - selectivity : selectivity;
      },
      [](const auto&) { return kUnknownSelectivity; },
  }, predicate);
}

double JoinSelectivity(const Expression& qual, int64_t lhs_card, int64_t rhs_card) {
  const auto* b = std::get_if<BinaryExpression>(&qual);
  if (!b) return 1.0;
  if (b->binop == BinaryOp::kAnd) {
    return JoinSelectivity(*b->lhs, lhs_card, rhs_card)
         * JoinSelectivity(*b->rhs, lhs_card, rhs_card);
  }
  if (b->binop == BinaryOp::kEq
      && std::holds_alternative<Attribute>(*b->lhs)
      && std::holds_alternative<Attribute>(*b->rhs)) {
    auto m = std::max<int64_t>(1, std::max(lhs_card, rhs_card));
    return 1.0 / static_cast<double>(m);
  }
  return 1.0;
}

}  // namespace

CardinalityEstimates::CardinalityEstimates(std::unordered_map<std::string, int64_t> table_sizes)
    : table_sizes_(std::move(table_sizes)) {}

int64_t CardinalityEstimates::GetCardinality(utils::NotNull<Group*> group) {
  if (auto it = cache_.find(group.get()); it != cache_.end()) {
    return it->second;
  }
  auto cardinality = GetCardinality(group->GetLogicalExprs().front()->root_operator);
  cache_[group.get()] = cardinality;
  return cardinality;
}

int64_t CardinalityEstimates::GetCardinality(const LogicalOperator& op) {
  return std::visit(utils::Overloaded{
      [this](const logical::Table& t) -> int64_t {
          if (auto it = table_sizes_.find(t.name); it != table_sizes_.end()) {
              return it->second;
          }
          return 10;
      },
      [this](const logical::Filter& f) -> int64_t {
          auto source_cardinality = GetCardinality(f.source);
          auto filtered = static_cast<int64_t>(
              source_cardinality * EstimateFilterSelectivity(f.predicate));
          return std::max<int64_t>(1, filtered);
      },
      [this](const logical::Projection& p) -> int64_t {
          return GetCardinality(p.source);
      },
      [this](const logical::Aggregation& a) -> int64_t {
          auto source_cardinality = GetCardinality(a.source);
          if (a.group_by.empty()) {
              return 1;
          }
          return source_cardinality;
      },
      [this](const logical::PartialAggregation& a) -> int64_t {
          auto source_cardinality = GetCardinality(a.source);
          if (a.group_by.empty()) {
              return 1;
          }
          return source_cardinality;
      },
      [this](const logical::FinalAggregation& a) -> int64_t {
          auto source_cardinality = GetCardinality(a.source);
          if (a.group_by.empty()) {
              return 1;
          }
          return source_cardinality;
      },
      [this](const logical::CrossJoin& j) -> int64_t {
          return GetCardinality(j.lhs) * GetCardinality(j.rhs);
      },
      [this](const logical::Join& j) -> int64_t {
          auto lhs_c = GetCardinality(j.lhs);
          auto rhs_c = GetCardinality(j.rhs);
          auto out = static_cast<double>(lhs_c) * static_cast<double>(rhs_c)
                   * JoinSelectivity(j.qual, lhs_c, rhs_c);
          return std::max<int64_t>(1, static_cast<int64_t>(out));
      },
  }, op);
}

}  // namespace stewkk::sql
