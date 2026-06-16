#include <stewkk/sql/models/parser/join_type.hpp>

namespace stewkk::sql {

std::string ToString(JoinType type) {
  switch (type) {
    case JoinType::kInner:
      return "⋈";
    case JoinType::kFull:
      return "⟗";
    case JoinType::kLeft:
      return "⟕";
    case JoinType::kRight:
      return "⟖";
  }
}

} // namespace stewkk::sql
