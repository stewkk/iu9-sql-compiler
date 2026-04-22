#pragma once

namespace stewkk::sql::utils {

template<class... Ts> struct Overloaded : Ts... { using Ts::operator()...; };

}  // namespace stewkk::sql::utils
