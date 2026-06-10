#pragma once

#include <array>
#include <memory>

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>
#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_split.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_merge.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_pushdown_through_projection.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_pushdown_through_join.hpp>
#include <stewkk/sql/logic/transformation_rules/in_to_or_chain.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_to_join_predicate.hpp>
#include <stewkk/sql/logic/transformation_rules/cross_join_to_join.hpp>
#include <stewkk/sql/logic/transformation_rules/filter_lift_through_join.hpp>
#include <stewkk/sql/logic/transformation_rules/projection_pushdown_through_join.hpp>
#include <stewkk/sql/logic/transformation_rules/outer_join_to_inner.hpp>
#include <stewkk/sql/logic/transformation_rules/aggregation_pushdown_through_join.hpp>
#include <stewkk/sql/logic/transformation_rules/aggregation_join_transpose.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_table.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_filter.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_projection.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_join.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_cross_join.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_hash_join.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_merge_join.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_aggregation.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_index_seek.hpp>

namespace stewkk::sql {

template<size_t NTransformation, size_t NImplementation>
struct Rules {
    std::array<std::unique_ptr<TransformationRule>, NTransformation> transformation_rules;
    std::array<std::unique_ptr<ImplementationRule>, NImplementation> implementation_rules;
};

Rules<14, 9> MakeMainRules(IndexCatalog indexes = {});
Rules<0, 6> MakeNaiveRules();

}  // namespace stewkk::sql
