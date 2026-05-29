#include <gmock/gmock.h>

#include <stewkk/sql/logic/executor/plan_serializer.hpp>

using ::testing::Eq;

namespace stewkk::sql {

namespace {

PhysicalPlanNode RoundTrip(const PhysicalPlanNode& node) {
    return Deserialize(Serialize(node));
}

} // namespace

TEST(PlanSerializerTest, SeqScan) {
    PhysicalPlanNode plan = SeqScan{"users"};
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
    EXPECT_THAT(Serialize(plan), Eq("(SeqScan users)"));
}

TEST(PlanSerializerTest, SeqScanAlias) {
    PhysicalPlanNode plan = SeqScan{"customer", "c"};
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
    EXPECT_THAT(Serialize(plan), Eq("(SeqScan customer c)"));
}

TEST(PlanSerializerTest, PhysicalFilter) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
        BinaryExpression{
            std::make_shared<Expression>(Attribute{"users", "age"}),
            BinaryOp::kLt,
            std::make_shared<Expression>(IntConst{10}),
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, PhysicalProjection) {
    PhysicalPlanNode plan = PhysicalProjection{
        std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
        {Attribute{"users", "id"}, Attribute{"users", "age"}},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, PhysicalProjectionEmpty) {
    PhysicalPlanNode plan = PhysicalProjection{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        {},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, NestedLoopCrossJoin) {
    PhysicalPlanNode plan = NestedLoopCrossJoin{
        std::make_shared<PhysicalPlanNode>(SeqScan{"users"}),
        std::make_shared<PhysicalPlanNode>(SeqScan{"books"}),
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, NestedLoopJoinInner) {
    PhysicalPlanNode plan = NestedLoopJoin{
        std::make_shared<PhysicalPlanNode>(SeqScan{"a"}),
        std::make_shared<PhysicalPlanNode>(SeqScan{"b"}),
        JoinType::kInner,
        BinaryExpression{
            std::make_shared<Expression>(Attribute{"a", "x"}),
            BinaryOp::kEq,
            std::make_shared<Expression>(Attribute{"b", "x"}),
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, NestedLoopJoinAllTypes) {
    for (auto type : {JoinType::kInner, JoinType::kFull, JoinType::kLeft, JoinType::kRight}) {
        PhysicalPlanNode plan = NestedLoopJoin{
            std::make_shared<PhysicalPlanNode>(SeqScan{"a"}),
            std::make_shared<PhysicalPlanNode>(SeqScan{"b"}),
            type,
            Literal::kTrue,
        };
        EXPECT_THAT(RoundTrip(plan), Eq(plan));
    }
}

TEST(PlanSerializerTest, ExprIntConst) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        IntConst{-42},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, ExprStringConst) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        StringConst{"Bob's Market \"North\""},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
    EXPECT_THAT(Serialize(plan), Eq("(PhysicalFilter (str \"Bob's Market \\\"North\\\"\") (SeqScan t))"));
}

TEST(PlanSerializerTest, ExprInList) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        InExpression{
            std::make_shared<Expression>(Attribute{"t", "region"}),
            {StringConst{"AMERICA"}, StringConst{"ASIA"}, Literal::kNull},
            false,
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
    EXPECT_THAT(Serialize(plan),
                Eq("(PhysicalFilter (in (attr t region) (values (str \"AMERICA\") (str \"ASIA\") NULL)) (SeqScan t))"));
}

TEST(PlanSerializerTest, ExprNotInList) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        InExpression{
            std::make_shared<Expression>(Attribute{"t", "id"}),
            {IntConst{1}, IntConst{2}},
            true,
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
    EXPECT_THAT(Serialize(plan), Eq("(PhysicalFilter (notin (attr t id) (values 1 2)) (SeqScan t))"));
}

TEST(PlanSerializerTest, ExprLiterals) {
    for (auto lit : {Literal::kNull, Literal::kTrue, Literal::kFalse, Literal::kUnknown}) {
        PhysicalPlanNode plan = PhysicalFilter{
            std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
            lit,
        };
        EXPECT_THAT(RoundTrip(plan), Eq(plan));
    }
}

TEST(PlanSerializerTest, ExprUnaryNot) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        UnaryExpression{UnaryOp::kNot, std::make_shared<Expression>(Attribute{"t", "x"})},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, ExprUnaryMinus) {
    PhysicalPlanNode plan = PhysicalFilter{
        std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
        UnaryExpression{UnaryOp::kMinus, std::make_shared<Expression>(IntConst{5})},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, ExprAllBinaryOps) {
    for (auto op : {BinaryOp::kGt, BinaryOp::kLt, BinaryOp::kLe, BinaryOp::kGe,
                    BinaryOp::kNotEq, BinaryOp::kEq, BinaryOp::kOr, BinaryOp::kAnd,
                    BinaryOp::kPlus, BinaryOp::kMinus, BinaryOp::kMul, BinaryOp::kDiv,
                    BinaryOp::kMod, BinaryOp::kPow}) {
        PhysicalPlanNode plan = PhysicalFilter{
            std::make_shared<PhysicalPlanNode>(SeqScan{"t"}),
            BinaryExpression{
                std::make_shared<Expression>(IntConst{1}),
                op,
                std::make_shared<Expression>(IntConst{2}),
            },
        };
        EXPECT_THAT(RoundTrip(plan), Eq(plan));
    }
}

TEST(PlanSerializerTest, DeepNestedPlan) {
    PhysicalPlanNode plan = PhysicalProjection{
        std::make_shared<PhysicalPlanNode>(PhysicalFilter{
            std::make_shared<PhysicalPlanNode>(NestedLoopJoin{
                std::make_shared<PhysicalPlanNode>(SeqScan{"employees"}),
                std::make_shared<PhysicalPlanNode>(SeqScan{"departments"}),
                JoinType::kLeft,
                BinaryExpression{
                    std::make_shared<Expression>(Attribute{"employees", "dept_id"}),
                    BinaryOp::kEq,
                    std::make_shared<Expression>(Attribute{"departments", "id"}),
                },
            }),
            BinaryExpression{
                std::make_shared<Expression>(Attribute{"departments", "id"}),
                BinaryOp::kGt,
                std::make_shared<Expression>(IntConst{3}),
            },
        }),
        {Attribute{"employees", "id"}, Attribute{"departments", "id"}},
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, IndexSeek) {
    PhysicalPlanNode plan = IndexSeek{
        "users",
        BinaryExpression{
            std::make_shared<Expression>(Attribute{"users", "id"}),
            BinaryOp::kGt,
            std::make_shared<Expression>(IntConst{42}),
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
    EXPECT_THAT(Serialize(plan), Eq("(IndexSeek (> (attr users id) 42) users)"));
}

TEST(PlanSerializerTest, HashJoin) {
    PhysicalPlanNode plan = HashJoin{
        std::make_shared<PhysicalPlanNode>(SeqScan{"a"}),
        std::make_shared<PhysicalPlanNode>(SeqScan{"b"}),
        JoinType::kInner,
        BinaryExpression{
            std::make_shared<Expression>(Attribute{"a", "id"}),
            BinaryOp::kEq,
            std::make_shared<Expression>(Attribute{"b", "id"}),
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerTest, MergeJoin) {
    PhysicalPlanNode plan = MergeJoin{
        std::make_shared<PhysicalPlanNode>(SeqScan{"a"}),
        std::make_shared<PhysicalPlanNode>(SeqScan{"b"}),
        JoinType::kLeft,
        BinaryExpression{
            std::make_shared<Expression>(Attribute{"a", "id"}),
            BinaryOp::kEq,
            std::make_shared<Expression>(Attribute{"b", "id"}),
        },
    };
    EXPECT_THAT(RoundTrip(plan), Eq(plan));
}

TEST(PlanSerializerDotTest, SmokeTest) {
    PhysicalPlanNode plan = NestedLoopJoin{
        std::make_shared<PhysicalPlanNode>(SeqScan{"a"}),
        std::make_shared<PhysicalPlanNode>(SeqScan{"b"}),
        JoinType::kInner,
        BinaryExpression{
            std::make_shared<Expression>(Attribute{"a", "x"}),
            BinaryOp::kEq,
            std::make_shared<Expression>(Attribute{"b", "x"}),
        },
    };
    auto dot = SerializeDot(plan);
    EXPECT_THAT(dot, ::testing::HasSubstr("digraph G"));
    EXPECT_THAT(dot, ::testing::HasSubstr("rankdir=BT"));
    EXPECT_THAT(dot, ::testing::HasSubstr("SeqScan"));
    EXPECT_THAT(dot, ::testing::HasSubstr("ON a.x = b.x"));
    EXPECT_THAT(dot, ::testing::HasSubstr("n0 -> n2"));
    EXPECT_THAT(dot, ::testing::HasSubstr("n1 -> n2"));
}

} // namespace stewkk::sql
