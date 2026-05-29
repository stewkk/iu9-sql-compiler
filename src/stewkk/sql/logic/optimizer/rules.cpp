#include <stewkk/sql/logic/optimizer/rules.hpp>

namespace stewkk::sql {

Rules<6, 7> MakeMainRules() {
    return {
        .transformation_rules = {
            std::make_unique<JoinCommutativity>(),
            std::make_unique<JoinAssociativity>(),
            std::make_unique<FilterSplit>(),
            std::make_unique<FilterMerge>(),
            std::make_unique<FilterPushdownThroughProjection>(),
            std::make_unique<FilterPushdownThroughJoin>(),
        },
        .implementation_rules = {
            std::make_unique<ImplementTable>(),
            std::make_unique<ImplementFilter>(),
            std::make_unique<ImplementProjection>(),
            std::make_unique<ImplementJoin>(),
            std::make_unique<ImplementCrossJoin>(),
            std::make_unique<ImplementHashJoin>(),
            std::make_unique<ImplementAggregation>(),
        },
    };
}

}  // namespace stewkk::sql
