#include <stewkk/sql/logic/optimizer/rules.hpp>

#include <utility>

namespace stewkk::sql {

Rules<14, 9> MakeMainRules(IndexCatalog indexes) {
    return {
        .transformation_rules = {
            std::make_unique<JoinCommutativity>(),
            std::make_unique<JoinAssociativity>(),
            std::make_unique<FilterSplit>(),
            std::make_unique<FilterMerge>(),
            std::make_unique<FilterPushdownThroughProjection>(),
            std::make_unique<FilterPushdownThroughJoin>(),
            std::make_unique<InToOrChain>(),
            std::make_unique<FilterToJoinPredicate>(),
            std::make_unique<CrossJoinToJoin>(),
            std::make_unique<FilterLiftThroughJoin>(),
            std::make_unique<ProjectionPushdownThroughJoin>(),
            std::make_unique<OuterJoinToInner>(),
            std::make_unique<AggregationPushdownThroughJoin>(),
            std::make_unique<AggregationJoinTranspose>(),
        },
        .implementation_rules = {
            std::make_unique<ImplementTable>(),
            std::make_unique<ImplementFilter>(),
            std::make_unique<ImplementIndexSeek>(std::move(indexes)),
            std::make_unique<ImplementProjection>(),
            std::make_unique<ImplementJoin>(),
            std::make_unique<ImplementCrossJoin>(),
            std::make_unique<ImplementHashJoin>(),
            std::make_unique<ImplementMergeJoin>(),
            std::make_unique<ImplementAggregation>(),
        },
    };
}

Rules<0, 6> MakeNaiveRules() {
    return {
        .transformation_rules = {},
        .implementation_rules = {
            std::make_unique<ImplementTable>(),
            std::make_unique<ImplementFilter>(),
            std::make_unique<ImplementProjection>(),
            std::make_unique<ImplementJoin>(),
            std::make_unique<ImplementCrossJoin>(),
            std::make_unique<ImplementAggregation>(),
        },
    };
}

}  // namespace stewkk::sql
