#pragma once

#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "dpe/concepts.hpp"
#include "dpe/fixed_string.hpp"

namespace dpe {

namespace detail {

template <typename... Ts>
struct type_list {};

template <fixed_string... Names>
struct unique_names : std::true_type {};

template <fixed_string Name, fixed_string... Rest>
struct unique_names<Name, Rest...>
    : std::bool_constant<((Name != Rest) && ...) && unique_names<Rest...>::value> {};

template <fixed_string... Names>
inline constexpr bool unique_names_v = unique_names<Names...>::value;

template <std::size_t I, typename T, typename... Ts>
struct nth_type_impl : nth_type_impl<I - 1U, Ts...> {};

template <typename T, typename... Ts>
struct nth_type_impl<0U, T, Ts...> {
  using type = T;
};

template <std::size_t I, typename... Ts>
using nth_type_t = typename nth_type_impl<I, Ts...>::type;

template <typename... Fields>
struct unique_field_names : std::true_type {};

template <typename Field, typename... Rest>
struct unique_field_names<Field, Rest...> : std::bool_constant<
                                                unique_names_v<Field::name, Rest::name...> &&
                                                unique_field_names<Rest...>::value> {};

template <typename... Fields>
inline constexpr bool unique_field_names_v = unique_field_names<Fields...>::value;

template <fixed_string Name, typename First, typename... Rest>
consteval auto schema_index_impl() -> std::size_t {
  if constexpr (First::name == Name) {
    return 0U;
  } else {
    static_assert(sizeof...(Rest) > 0U, "Requested column is not part of this schema.");
    return 1U + schema_index_impl<Name, Rest...>();
  }
}

template <typename Schema>
struct schema_size;

template <typename... Fields>
struct schema_size<type_list<Fields...>> {
  static constexpr auto value = sizeof...(Fields);
};

template <typename T>
struct is_schema : std::false_type {};

template <typename... Fields>
struct is_schema<type_list<Fields...>> : std::true_type {};

}  // namespace detail

template <fixed_string Name, supported_storage_value T>
struct field {
  using value_type = T;
  static constexpr auto name = Name;
};

template <typename... Fields>
struct schema {
  static_assert((supported_storage_value<typename Fields::value_type> && ...),
                "Schema fields must use engine-supported logical column types.");
  static_assert(detail::unique_field_names_v<Fields...>,
                "Schema field names must be unique at compile time.");

  using fields = detail::type_list<Fields...>;
  static constexpr auto column_count = sizeof...(Fields);

  template <fixed_string Name>
  static consteval auto contains() -> bool {
    return ((Fields::name == Name) || ...);
  }

  template <fixed_string Name>
  static consteval auto index_of() -> std::size_t {
    static_assert(contains<Name>(), "Requested column is not part of this schema.");
    return detail::schema_index_impl<Name, Fields...>();
  }

  template <std::size_t I>
  using field_at = detail::nth_type_t<I, Fields...>;
};

template <typename T>
struct is_schema : std::false_type {};

template <typename... Fields>
struct is_schema<schema<Fields...>> : std::true_type {};

template <typename T>
inline constexpr bool is_schema_v = is_schema<T>::value;

template <std::size_t I, typename Schema>
struct schema_field;

template <std::size_t I, typename... Fields>
struct schema_field<I, schema<Fields...>> {
  using type = detail::nth_type_t<I, Fields...>;
};

template <std::size_t I, typename Schema>
using schema_field_t = typename schema_field<I, Schema>::type;

template <fixed_string Name, typename Schema>
struct column_type;

template <fixed_string Name, typename... Fields>
struct column_type<Name, schema<Fields...>> {
  using type = typename detail::nth_type_t<schema<Fields...>::template index_of<Name>(),
                                           Fields...>::value_type;
};

template <fixed_string Name, typename Schema>
using column_type_t = typename column_type<Name, Schema>::type;

template <fixed_string Name, typename Schema>
inline constexpr bool schema_contains_v = Schema::template contains<Name>();

template <typename Schema, typename Fn, std::size_t... I>
constexpr void for_each_field_impl(Fn&& fn, std::index_sequence<I...>) {
  (fn.template operator()<schema_field_t<I, Schema>>(), ...);
}

template <typename Schema, typename Fn>
constexpr void for_each_field(Fn&& fn) {
  for_each_field_impl<Schema>(std::forward<Fn>(fn),
                              std::make_index_sequence<Schema::column_count>{});
}

}  // namespace dpe
