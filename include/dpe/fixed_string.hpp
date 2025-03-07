#pragma once

#include <algorithm>
#include <array>
#include <compare>
#include <cstddef>
#include <string_view>

namespace dpe {

template <std::size_t N>
struct fixed_string {
  std::array<char, N> chars{};

  constexpr fixed_string(const char (&value)[N]) {
    std::copy_n(value, N, chars.begin());
  }

  [[nodiscard]] constexpr auto data() const noexcept -> const char* {
    return chars.data();
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return N - 1U;
  }

  [[nodiscard]] constexpr auto view() const noexcept -> std::string_view {
    return {chars.data(), N - 1U};
  }

  template <std::size_t M>
  [[nodiscard]] constexpr auto operator==(const fixed_string<M>& other) const noexcept -> bool {
    return view() == other.view();
  }

  template <std::size_t M>
  [[nodiscard]] constexpr auto operator<=>(const fixed_string<M>& other) const noexcept {
    return view() <=> other.view();
  }
};

template <std::size_t N>
fixed_string(const char (&)[N]) -> fixed_string<N>;

template <fixed_string Value>
struct name_tag {
  static constexpr auto value = Value;
};

template <fixed_string Value>
inline constexpr auto name_v = name_tag<Value>{};

template <fixed_string... Names>
struct fixed_string_list {};

}  // namespace dpe
