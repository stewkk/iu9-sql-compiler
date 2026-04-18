#pragma once

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

class Optimizer {
    public:
        Operator Optimize(Operator expression);
};

}  // namespace stewkk::sql
