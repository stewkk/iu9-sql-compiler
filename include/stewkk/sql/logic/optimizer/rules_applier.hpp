#pragma once

#include <array>
#include <unordered_map>

#include <stewkk/sql/logic/optimizer/rules.hpp>

namespace stewkk::sql {

template<size_t NTransformation, size_t NImplementation>
class RulesApplier {
public:

    
private:
    Rules<NTransformation, NImplementation> rules_;
    std::unordered_map<LogicalExpr*, std::array<char, NTransformation>> applied_transformation_rules_;
};

} // stewkk::sql