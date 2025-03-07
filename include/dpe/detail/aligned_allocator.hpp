#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <new>

namespace dpe::detail {

template <typename T, std::size_t Alignment = 64U>
class aligned_allocator {
 public:
  using value_type = T;

  aligned_allocator() noexcept = default;

  template <typename U>
  constexpr aligned_allocator(const aligned_allocator<U, Alignment>&) noexcept {}

  [[nodiscard]] auto allocate(std::size_t count) -> T* {
    if (count > (std::numeric_limits<std::size_t>::max() / sizeof(T))) {
      throw std::bad_array_new_length{};
    }

    return static_cast<T*>(::operator new(count * sizeof(T), std::align_val_t{Alignment}));
  }

  void deallocate(T* pointer, std::size_t) noexcept {
    ::operator delete(pointer, std::align_val_t{Alignment});
  }

  template <typename U>
  struct rebind {
    using other = aligned_allocator<U, Alignment>;
  };

  using propagate_on_container_move_assignment = std::true_type;
};

template <typename T, typename U, std::size_t Alignment>
constexpr auto operator==(const aligned_allocator<T, Alignment>&,
                          const aligned_allocator<U, Alignment>&) noexcept -> bool {
  return true;
}

}  // namespace dpe::detail
