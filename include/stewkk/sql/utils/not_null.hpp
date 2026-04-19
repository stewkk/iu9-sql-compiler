#pragma once

#include <cassert>
#include <type_traits>

namespace stewkk::sql::utils {

template<class T>
  requires std::is_pointer_v<T>
class NotNull {
  public:
    NotNull(T ptr) : ptr_(ptr) { assert(ptr_ != nullptr); }  // NOLINT(google-explicit-constructor)
    NotNull(std::nullptr_t) = delete;

    T get() const { return ptr_; }
    auto& operator*()  const { return *ptr_; }
    T     operator->() const { return ptr_; }

    operator T() const { return ptr_; }  // NOLINT(google-explicit-constructor)

  private:
    T ptr_;
};

}  // namespace stewkk::sql::utils
