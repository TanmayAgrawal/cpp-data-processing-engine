#pragma once

#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

namespace dpe {

template <typename T>
using bare_t = std::remove_cvref_t<T>;

template <typename T>
inline constexpr bool always_false_v = false;

template <typename T>
concept supported_numeric =
    std::same_as<bare_t<T>, std::int32_t> || std::same_as<bare_t<T>, std::int64_t> ||
    std::same_as<bare_t<T>, float> || std::same_as<bare_t<T>, double>;

template <typename T>
concept supported_storage_value =
    supported_numeric<T> || std::same_as<bare_t<T>, std::string>;

template <typename T>
concept string_like =
    std::same_as<bare_t<T>, std::string> || std::same_as<bare_t<T>, std::string_view>;

template <typename T>
concept numeric_like =
    (std::integral<bare_t<T>> || std::floating_point<bare_t<T>>) &&
    !std::same_as<bare_t<T>, bool>;

template <typename T>
struct expression_view_type {
  using type = bare_t<T>;
};

template <>
struct expression_view_type<std::string> {
  using type = std::string_view;
};

template <typename T>
using expression_view_type_t = typename expression_view_type<bare_t<T>>::type;

template <typename T>
struct normalized_storage_type {
 private:
  using U = bare_t<T>;

 public:
  using type = std::conditional_t<
      std::same_as<U, std::string> || std::same_as<U, std::string_view>, std::string,
      std::conditional_t<
          std::same_as<U, float>, float,
          std::conditional_t<
              std::same_as<U, double>, double,
              std::conditional_t<
                  std::signed_integral<U> && (sizeof(U) == 4U), std::int32_t,
                  std::conditional_t<std::signed_integral<U> && (sizeof(U) == 8U),
                                     std::int64_t, void>>>>>;
};

template <typename T>
using normalized_storage_type_t = typename normalized_storage_type<T>::type;

template <typename T>
concept materializable_expression =
    supported_storage_value<normalized_storage_type_t<T>>;

}  // namespace dpe
