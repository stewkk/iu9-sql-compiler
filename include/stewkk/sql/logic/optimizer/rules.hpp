#pragma once

#include <array>
#include <memory>

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>
#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_table.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_filter.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_projection.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_join.hpp>
#include <stewkk/sql/logic/implementation_rules/implement_cross_join.hpp>

namespace stewkk::sql {

template<size_t NTransformation, size_t NImplementation>
struct Rules {
    std::array<std::unique_ptr<TransformationRule>, NTransformation> transformation_rules;
    std::array<std::unique_ptr<ImplementationRule>, NImplementation> implementation_rules;
};

Rules<2, 5> MakeMainRules();

}  // namespace stewkk::sql
