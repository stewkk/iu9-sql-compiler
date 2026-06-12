#pragma once

#include <string>

namespace stewkk::sql {

enum class JoinType {
    kInner,
    kFull,
    kLeft,
    kRight,
};

std::string ToString(JoinType type);

}  // namespace stewkk::sql
