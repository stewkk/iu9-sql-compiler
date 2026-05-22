#include <gmock/gmock.h>

#include <stewkk/sql/logic/optimizer/properties/property_set.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

using ::testing::IsTrue;
using ::testing::IsFalse;

namespace stewkk::sql {

TEST(SortOrderTest, EmptyRequiredAlwaysSatisfied) {
  SortOrder delivered{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kAsc}}};
  SortOrder required{};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

TEST(SortOrderTest, ExactMatchSatisfied) {
  SortOrder delivered{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kDesc}}};
  SortOrder required{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kDesc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

TEST(SortOrderTest, LongerDeliveredSatisfiesPrefix) {
  SortOrder delivered{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kAsc}, {"t", "c", Direction::kAsc}}};
  SortOrder required{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kAsc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

TEST(SortOrderTest, ShorterDeliveredDoesNotSatisfy) {
  SortOrder delivered{{{"t", "a", Direction::kAsc}}};
  SortOrder required{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kAsc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsFalse());
}

TEST(SortOrderTest, WrongDirectionDoesNotSatisfy) {
  SortOrder delivered{{{"t", "a", Direction::kAsc}}};
  SortOrder required{{{"t", "a", Direction::kDesc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsFalse());
}

TEST(SortOrderTest, WrongColumnDoesNotSatisfy) {
  SortOrder delivered{{{"t", "b", Direction::kAsc}}};
  SortOrder required{{{"t", "a", Direction::kAsc}}};
  ASSERT_THAT(delivered.Satisfies(required), IsFalse());
}

TEST(PropertySetTest, AnyAlwaysSatisfied) {
  PropertySet delivered{SortOrder{{{"t", "a", Direction::kAsc}}}};
  ASSERT_THAT(delivered.Satisfies(PropertySet::Any()), IsTrue());
}

TEST(PropertySetTest, AnySatisfiesAny) {
  ASSERT_THAT(PropertySet::Any().Satisfies(PropertySet::Any()), IsTrue());
}

TEST(PropertySetTest, AnyDoesNotSatisfySortRequirement) {
  PropertySet required{SortOrder{{{"t", "a", Direction::kAsc}}}};
  ASSERT_THAT(PropertySet::Any().Satisfies(required), IsFalse());
}

TEST(PropertySetTest, SortSatisfiesSortPrefix) {
  PropertySet delivered{SortOrder{{{"t", "a", Direction::kAsc}, {"t", "b", Direction::kAsc}}}};
  PropertySet required{SortOrder{{{"t", "a", Direction::kAsc}}}};
  ASSERT_THAT(delivered.Satisfies(required), IsTrue());
}

}  // namespace stewkk::sql
