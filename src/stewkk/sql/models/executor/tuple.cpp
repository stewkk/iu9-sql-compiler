#include <stewkk/sql/models/executor/tuple.hpp>

namespace stewkk::sql {

NonNullValue GetTrileanValue(Trilean v) {
    NonNullValue res;
    res.trilean_value = v;
    return res;
}

}  // namespace stewkk::sql
