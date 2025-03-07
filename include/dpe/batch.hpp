#pragma once

#include <array>
#include <ranges>
#include <span>
#include <stdexcept>
#include <tuple>
#include <type_traits>

#include "dpe/schema.hpp"

namespace dpe {

template <typename T>
struct batch_column_view {
  std::span<const T> values{};

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return values.size();
  }

  [[nodiscard]] constexpr auto operator[](std::size_t row) const noexcept -> const T& {
    return values[row];
  }
};

template <>
struct batch_column_view<std::string> {
  std::span<const std::string_view> values{};

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return values.size();
  }

  [[nodiscard]] constexpr auto operator[](std::size_t row) const noexcept -> std::string_view {
    return values[row];
  }
};

namespace detail {

template <typename Schema>
struct batch_tuple_type;

template <typename... Fields>
struct batch_tuple_type<schema<Fields...>> {
  using type = std::tuple<batch_column_view<typename Fields::value_type>...>;
};

template <typename Schema>
using batch_tuple_type_t = typename batch_tuple_type<Schema>::type;

constexpr auto as_batch_view(std::span<const std::string_view> span)
    -> batch_column_view<std::string> {
  return {span};
}

constexpr auto as_batch_view(std::span<std::string_view> span) -> batch_column_view<std::string> {
  return {std::span<const std::string_view>{span.data(), span.size()}};
}

template <typename T>
constexpr auto as_batch_view(std::span<const T> span) -> batch_column_view<T> {
  return {span};
}

template <typename T>
constexpr auto as_batch_view(std::span<T> span) -> batch_column_view<T> {
  return {std::span<const T>{span.data(), span.size()}};
}

template <std::ranges::contiguous_range Range>
  requires std::same_as<bare_t<std::ranges::range_value_t<Range>>, std::string_view>
constexpr auto as_batch_view(Range&& range) -> batch_column_view<std::string> {
  return {std::span<const std::string_view>{std::data(range), std::size(range)}};
}

template <std::ranges::contiguous_range Range>
  requires(!std::same_as<bare_t<std::ranges::range_value_t<Range>>, std::string_view> &&
           !std::same_as<bare_t<std::ranges::range_value_t<Range>>, std::string>)
constexpr auto as_batch_view(Range&& range)
    -> batch_column_view<bare_t<std::ranges::range_value_t<Range>>> {
  using value_type = bare_t<std::ranges::range_value_t<Range>>;
  return {std::span<const value_type>{std::data(range), std::size(range)}};
}

template <typename Tuple, std::size_t... I>
constexpr auto validate_batch_sizes(const Tuple& columns, std::index_sequence<I...>)
    -> std::size_t {
  const std::array<std::size_t, sizeof...(I)> sizes{std::get<I>(columns).size()...};
  if constexpr (sizeof...(I) == 0U) {
    return 0U;
  } else {
    const auto expected = sizes.front();
    for (const auto size : sizes) {
      if (size != expected) {
        throw std::invalid_argument("All batch columns must have the same row count.");
      }
    }
    return expected;
  }
}

}  // namespace detail

template <typename Schema>
struct batch {
  using schema_type = Schema;
  using columns_type = detail::batch_tuple_type_t<Schema>;

  columns_type columns{};
  std::size_t row_count{0U};

  template <fixed_string Name>
  [[nodiscard]] constexpr auto column() const noexcept {
    return std::get<Schema::template index_of<Name>()>(columns);
  }

  template <fixed_string Name>
  [[nodiscard]] constexpr auto column(name_tag<Name>) const noexcept {
    return column<Name>();
  }
};

template <typename Schema, typename... Views>
constexpr auto make_batch(Views&&... views) -> batch<Schema> {
  static_assert(sizeof...(Views) == Schema::column_count,
                "Batch arity must match the schema column count.");

  auto columns =
      typename batch<Schema>::columns_type{detail::as_batch_view(std::forward<Views>(views))...};
  const auto row_count = detail::validate_batch_sizes(
      columns, std::make_index_sequence<Schema::column_count>{});

  return batch<Schema>{
      .columns = std::move(columns),
      .row_count = row_count};
}

}  // namespace dpe
