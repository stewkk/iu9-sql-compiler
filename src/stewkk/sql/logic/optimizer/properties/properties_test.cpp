#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

using ::testing::IsTrue;
using ::testing::IsFalse;

namespace stewkk::sql {

TEST(SortOrderTest, EmptyRequiredAlwaysSatisfied) {
  SortOrder delivered{{{"a", Direction::kAsc}, {"b", Direction::kAsc}}};
  SortOrder required{};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

TEST(SortOrderTest, ExactMatchSatisfied) {
  SortOrder delivered{{{"a", Direction::kAsc}, {"b", Direction::kDesc}}};
  SortOrder required{{{"a", Direction::kAsc}, {"b", Direction::kDesc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

TEST(SortOrderTest, LongerDeliveredSatisfiesPrefix) {
  SortOrder delivered{{{"a", Direction::kAsc}, {"b", Direction::kAsc}, {"c", Direction::kAsc}}};
  SortOrder required{{{"a", Direction::kAsc}, {"b", Direction::kAsc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

TEST(SortOrderTest, ShorterDeliveredDoesNotSatisfy) {
  SortOrder delivered{{{"a", Direction::kAsc}}};
  SortOrder required{{{"a", Direction::kAsc}, {"b", Direction::kAsc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsFalse());
}

TEST(SortOrderTest, WrongDirectionDoesNotSatisfy) {
  SortOrder delivered{{{"a", Direction::kAsc}}};
  SortOrder required{{{"a", Direction::kDesc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsFalse());
}

TEST(SortOrderTest, WrongColumnDoesNotSatisfy) {
  SortOrder delivered{{{"b", Direction::kAsc}}};
  SortOrder required{{{"a", Direction::kAsc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsFalse());
}

TEST(PropertySetTest, AnyAlwaysSatisfied) {
  PropertySet delivered{SortOrder{{{"a", Direction::kAsc}}}};
  ASSERT_THAT(delivered.Satisfies(PropertySet::Any()), IsTrue());
}

TEST(PropertySetTest, AnySatisfiesAny) {
  ASSERT_THAT(PropertySet::Any().Satisfies(PropertySet::Any()), IsTrue());
}

TEST(PropertySetTest, AnyDoesNotSatisfySortRequirement) {
  PropertySet required{SortOrder{{{"a", Direction::kAsc}}}};
  ASSERT_THAT(PropertySet::Any().Satisfies(required), IsFalse());
}

TEST(PropertySetTest, SortSatisfiesSortPrefix) {
  PropertySet delivered{SortOrder{{{"a", Direction::kAsc}, {"b", Direction::kAsc}}}};
  PropertySet required{SortOrder{{{"a", Direction::kAsc}}}};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

}  // namespace stewkk::sql
