#pragma once

#include <print>

namespace stewkk::sql {

inline constexpr bool kDebug =
#ifdef DEBUG
    true;
#else
    false;
#endif

template <typename... Args>
void Log(std::format_string<Args...> fmt, Args&&... args) {
    if constexpr (kDebug) {
        std::println(stderr, fmt, std::forward<Args>(args)...);
    }
}

}  // namespace stewkk::sql
