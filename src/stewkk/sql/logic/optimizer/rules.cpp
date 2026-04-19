#include <stewkk/sql/logic/optimizer/rules.hpp>

namespace stewkk::sql {

Rules<2, 0> MakeMainRules() {
    return {
        .transformation_rules = {
            std::make_unique<JoinCommutativity>(),
            std::make_unique<JoinAssociativity>(),
        },
        .implementation_rules = {},
    };
}

}  // namespace stewkk::sql
