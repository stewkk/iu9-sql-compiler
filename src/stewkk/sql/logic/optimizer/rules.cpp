#include <stewkk/sql/logic/optimizer/rules.hpp>

namespace stewkk::sql {

Rules<2, 5> MakeMainRules() {
    return {
        .transformation_rules = {
            std::make_unique<JoinCommutativity>(),
            std::make_unique<JoinAssociativity>(),
        },
        .implementation_rules = {
            std::make_unique<ImplementTable>(),
            std::make_unique<ImplementFilter>(),
            std::make_unique<ImplementProjection>(),
            std::make_unique<ImplementJoin>(),
            std::make_unique<ImplementCrossJoin>(),
        },
    };
}

}  // namespace stewkk::sql
