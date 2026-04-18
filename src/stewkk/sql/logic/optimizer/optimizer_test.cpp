#include <gmock/gmock.h>

#include <unordered_map>
#include <ranges>

#include <stewkk/sql/logic/parser/parser.hpp>
#include <stewkk/sql/logic/optimizer/optimizer.hpp>
#include <stewkk/sql/logic/result/result.hpp>

using ::testing::Eq;

namespace stewkk::sql {

TEST(OptimizerTest, Simple) {
  std::stringstream s{"SELECT * FROM users;"};
  Operator op = GetAST(s).value();
  Optimizer optimizer;

  auto got = optimizer.Optimize(op);

  ASSERT_EQ(got, op);
}

template<class... Ts> struct Overloaded : Ts... { using Ts::operator()...; };

using GroupKey = std::string;

namespace logical {

using Table = Table;

struct Filter {
  GroupKey source;
  Expression predicate;
};

struct Projection {
  GroupKey source;
  std::vector<Expression> expressions;
};

struct CrossJoin {
  GroupKey lhs;
  GroupKey rhs;
};

struct Join {
  GroupKey lhs;
  GroupKey rhs;
  using Type = JoinType;
  Type type;
  Expression qual;
};

} // namespace logical


using LogicalExpr = std::variant<logical::Table, logical::Filter, logical::Projection, logical::Join, logical::CrossJoin>;

class Group {
  public:
    explicit Group(LogicalExpr expr) : logical_exprs_({std::move(expr)}) {}
  private:
    std::vector<LogicalExpr> logical_exprs_;
};

class Memo {
 public:
  explicit Memo(const Operator& expr) : mapping_(), root_(AddGroup(expr)) {}

  private:

  GroupKey AddGroup(const Operator& expr) {
    return std::visit(Overloaded{
      [this](const Table& t) {
        auto key = std::format("Table({})", t.name);
        mapping_.insert_or_assign(key, Group(logical::Table(t)));
        return key;
      },
      [this](const Filter& f) {
        auto source_group = AddGroup(*f.source);
        auto key = std::format("Filter({}, {})", source_group, ToString(f.expr));
        mapping_.insert_or_assign(key, Group(logical::Filter(source_group, f.expr)));
        return key;
      },
      [this](const Projection& p) {
        auto source_group = AddGroup(*p.source);
        auto exprs = p.expressions | std::views::transform([](const Expression& e) { return ToString(e); }) | std::views::join_with(',') | std::ranges::to<std::string>();
        auto key = std::format("Projection({}, {})", source_group, exprs);
        mapping_.insert_or_assign(key, Group(logical::Projection(source_group, p.expressions)));
        return key;
      },
      [this](const CrossJoin& j) {
        auto lhs_group = AddGroup(*j.lhs);
        auto rhs_group = AddGroup(*j.rhs);
        auto key = std::format("CrossJoin({}, {})", lhs_group, rhs_group);
        mapping_.insert_or_assign(key, Group(logical::CrossJoin(lhs_group, rhs_group)));
        return key;
      },
      [this](const Join& j) {
        auto lhs_group = AddGroup(*j.lhs);
        auto rhs_group = AddGroup(*j.rhs);
        auto key = std::format("Join({}, {}, {}, {})", lhs_group, rhs_group, ToString(j.type), ToString(j.qual));
        mapping_.insert_or_assign(key, Group(logical::Join(lhs_group, rhs_group, j.type, j.qual)));
        return key;
      },
    }, expr);
  }

  std::unordered_map<GroupKey, Group> mapping_;
  GroupKey root_;
};

// DSU data-structure needed?

TEST(OptimizerTest, GetGroup) {
  Operator join_ab = Join{
      .type = JoinType::kInner,
      .qual = Literal::kTrue,
      .lhs = std::make_shared<Operator>(Table{.name = "A"}),
      .rhs = std::make_shared<Operator>(Table{.name = "B"}),
  };

  Memo memo(join_ab);

  // Operator join_ba = Join{
  //     .type = JoinType::kInner,
  //     .qual = Literal::kTrue,
  //     .lhs = std::make_shared<Operator>(Table{.name = "B"}),
  //     .rhs = std::make_shared<Operator>(Table{.name = "A"}),
  // };

  // ASSERT_EQ(*memo.GetGroup(join_ab), *memo.GetGroup(join_ba));
}

}  // namespace stewkk::sql
