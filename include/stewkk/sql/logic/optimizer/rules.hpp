#pragma once

#include <array>
#include <memory>

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/transformation_rules/join_commutativity.hpp>
#include <stewkk/sql/logic/transformation_rules/join_associativity.hpp>

namespace stewkk::sql {

template<size_t NTransformation, size_t NImplementation>
struct Rules {
    std::array<std::unique_ptr<TransformationRule>, NTransformation> transformation_rules;
    std::array<std::unique_ptr<ImplementationRule>, NImplementation> implementation_rules;
};

Rules<2, 0> MakeMainRules();

}  // namespace stewkk::sql
