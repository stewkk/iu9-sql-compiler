#pragma once

#include <stewkk/sql/utils/not_null.hpp>

namespace stewkk::sql {

struct Group;

namespace physical {

} // namespace physical

struct PhysicalExpr {
    utils::NotNull<Group*> group;
};

}  // namespace stewkk::sql
