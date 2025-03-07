#pragma once

#include <concepts>
#include <cstddef>
#include <string_view>
#include <type_traits>
#include <utility>

#include "dpe/concepts.hpp"
#include "dpe/fixed_string.hpp"
#include "dpe/schema.hpp"

namespace dpe {

struct expression_tag {};

template <typename T>
concept expression_node = std::derived_from<bare_t<T>, expression_tag>;

template <typename Expr, typename Schema>
using expression_value_t = typename bare_t<Expr>::template result_type<Schema>;

template <typename Expr, typename Schema>
concept well_formed_expression_for = requires { typename expression_value_t<Expr, Schema>; };

template <typename Expr, typename Schema>
concept boolean_expression_for =
    well_formed_expression_for<Expr, Schema> &&
    std::same_as<bare_t<expression_value_t<Expr, Schema>>, bool>;

template <typename Expr, typename Schema>
concept projection_expression_for =
    well_formed_expression_for<Expr, Schema> &&
    materializable_expression<expression_value_t<Expr, Schema>>;

namespace ops {

struct plus {
  template <numeric_like L, numeric_like R>
  constexpr auto operator()(L lhs, R rhs) const -> decltype(lhs + rhs) {
    return lhs + rhs;
  }
};

struct minus {
  template <numeric_like L, numeric_like R>
  constexpr auto operator()(L lhs, R rhs) const -> decltype(lhs - rhs) {
    return lhs - rhs;
  }
};

struct multiplies {
  template <numeric_like L, numeric_like R>
  constexpr auto operator()(L lhs, R rhs) const -> decltype(lhs * rhs) {
    return lhs * rhs;
  }
};

struct divides {
  template <numeric_like L, numeric_like R>
  constexpr auto operator()(L lhs, R rhs) const -> decltype(lhs / rhs) {
    return lhs / rhs;
  }
};

struct greater {
  template <typename L, typename R>
    requires std::totally_ordered_with<L, R>
  constexpr auto operator()(const L& lhs, const R& rhs) const -> bool {
    return lhs > rhs;
  }
};

struct greater_equal {
  template <typename L, typename R>
    requires std::totally_ordered_with<L, R>
  constexpr auto operator()(const L& lhs, const R& rhs) const -> bool {
    return lhs >= rhs;
  }
};

struct less {
  template <typename L, typename R>
    requires std::totally_ordered_with<L, R>
  constexpr auto operator()(const L& lhs, const R& rhs) const -> bool {
    return lhs < rhs;
  }
};

struct less_equal {
  template <typename L, typename R>
    requires std::totally_ordered_with<L, R>
  constexpr auto operator()(const L& lhs, const R& rhs) const -> bool {
    return lhs <= rhs;
  }
};

struct equal {
  template <typename L, typename R>
    requires requires(const L& lhs, const R& rhs) {
      { lhs == rhs } -> std::convertible_to<bool>;
    }
  constexpr auto operator()(const L& lhs, const R& rhs) const -> bool {
    return lhs == rhs;
  }
};

struct not_equal {
  template <typename L, typename R>
    requires requires(const L& lhs, const R& rhs) {
      { lhs != rhs } -> std::convertible_to<bool>;
    }
  constexpr auto operator()(const L& lhs, const R& rhs) const -> bool {
    return lhs != rhs;
  }
};

struct logical_and {
  template <typename L, typename R>
    requires std::convertible_to<L, bool> && std::convertible_to<R, bool>
  constexpr auto operator()(L lhs, R rhs) const -> bool {
    return static_cast<bool>(lhs) && static_cast<bool>(rhs);
  }
};

struct logical_or {
  template <typename L, typename R>
    requires std::convertible_to<L, bool> && std::convertible_to<R, bool>
  constexpr auto operator()(L lhs, R rhs) const -> bool {
    return static_cast<bool>(lhs) || static_cast<bool>(rhs);
  }
};

struct logical_not {
  template <typename T>
    requires std::convertible_to<T, bool>
  constexpr auto operator()(T value) const -> bool {
    return !static_cast<bool>(value);
  }
};

}  // namespace ops

template <fixed_string Name>
struct column_ref : expression_tag {
  static constexpr auto name = Name;

  template <typename Schema>
  using result_type = expression_view_type_t<column_type_t<Name, Schema>>;

  template <typename Snapshot>
  [[nodiscard]] constexpr auto eval(const Snapshot& snapshot, std::size_t row) const
      -> result_type<typename Snapshot::schema_type> {
    return snapshot.template value<Name>(row);
  }
};

template <typename T>
struct literal : expression_tag {
  T value;

  template <typename Schema>
  using result_type = T;

  template <typename Snapshot>
  [[nodiscard]] constexpr auto eval(const Snapshot&, std::size_t) const -> const T& {
    return value;
  }
};

template <typename Op, typename Expr>
struct unary_expr : expression_tag {
  Expr expr;

  template <typename Schema>
  using result_type =
      decltype(Op{}(std::declval<expression_value_t<Expr, Schema>>()));

  template <typename Snapshot>
  [[nodiscard]] constexpr auto eval(const Snapshot& snapshot, std::size_t row) const
      -> result_type<typename Snapshot::schema_type> {
    return Op{}(expr.eval(snapshot, row));
  }
};

template <typename Op, typename Left, typename Right>
struct binary_expr : expression_tag {
  Left lhs;
  Right rhs;

  template <typename Schema>
  using result_type = decltype(
      Op{}(std::declval<expression_value_t<Left, Schema>>(),
           std::declval<expression_value_t<Right, Schema>>()));

  template <typename Snapshot>
  [[nodiscard]] constexpr auto eval(const Snapshot& snapshot, std::size_t row) const
      -> result_type<typename Snapshot::schema_type> {
    return Op{}(lhs.eval(snapshot, row), rhs.eval(snapshot, row));
  }
};

template <fixed_string Alias, typename Expr>
struct named_expression {
  using expr_type = Expr;
  static constexpr auto name = Alias;

  Expr expr;
};

namespace detail {

template <typename T>
constexpr auto make_literal(T&& value) {
  return literal<bare_t<T>>{.value = std::forward<T>(value)};
}

template <std::size_t N>
constexpr auto make_literal(const char (&value)[N]) {
  return literal<std::string_view>{.value = {value, N - 1U}};
}

inline constexpr auto make_literal(const char* value) {
  return literal<std::string_view>{
      .value = value == nullptr ? std::string_view{} : std::string_view{value}};
}

inline constexpr auto make_literal(char* value) {
  return literal<std::string_view>{
      .value = value == nullptr ? std::string_view{} : std::string_view{value}};
}

template <typename T>
constexpr auto as_expression(T&& value) {
  if constexpr (expression_node<T>) {
    return std::forward<T>(value);
  } else {
    return make_literal(std::forward<T>(value));
  }
}

template <typename T>
using as_expression_t = bare_t<decltype(as_expression(std::declval<T>()))>;

template <typename Left, typename Right, typename Op>
constexpr auto make_binary(Left&& lhs, Right&& rhs, Op) {
  auto lhs_expr = as_expression(std::forward<Left>(lhs));
  auto rhs_expr = as_expression(std::forward<Right>(rhs));
  return binary_expr<Op, bare_t<decltype(lhs_expr)>, bare_t<decltype(rhs_expr)>>{
      .lhs = std::move(lhs_expr),
      .rhs = std::move(rhs_expr)};
}

template <typename Expr, typename Op>
constexpr auto make_unary(Expr&& expr, Op) {
  auto expression = as_expression(std::forward<Expr>(expr));
  return unary_expr<Op, bare_t<decltype(expression)>>{.expr = std::move(expression)};
}

}  // namespace detail

template <fixed_string Name>
[[nodiscard]] constexpr auto col() -> column_ref<Name> {
  return {};
}

template <fixed_string Name>
[[nodiscard]] constexpr auto col(name_tag<Name>) -> column_ref<Name> {
  return {};
}

template <fixed_string Alias, typename Expr>
[[nodiscard]] constexpr auto as(Expr&& expr)
    -> named_expression<Alias, detail::as_expression_t<Expr&&>> {
  auto expression = detail::as_expression(std::forward<Expr>(expr));
  return {std::move(expression)};
}

template <fixed_string Alias, typename Expr>
[[nodiscard]] constexpr auto as(name_tag<Alias>, Expr&& expr)
    -> named_expression<Alias, detail::as_expression_t<Expr&&>> {
  return as<Alias>(std::forward<Expr>(expr));
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator+(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs), ops::plus{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator-(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs), ops::minus{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator*(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs),
                             ops::multiplies{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator/(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs), ops::divides{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator>(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs), ops::greater{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator>=(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs),
                             ops::greater_equal{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator<(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs), ops::less{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator<=(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs),
                             ops::less_equal{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator==(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs), ops::equal{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator!=(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs),
                             ops::not_equal{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator&&(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs),
                             ops::logical_and{});
}

template <typename Left, typename Right>
  requires(expression_node<Left> || expression_node<Right>)
[[nodiscard]] constexpr auto operator||(Left&& lhs, Right&& rhs) {
  return detail::make_binary(std::forward<Left>(lhs), std::forward<Right>(rhs),
                             ops::logical_or{});
}

template <typename Expr>
  requires expression_node<Expr>
[[nodiscard]] constexpr auto operator!(Expr&& expr) {
  return detail::make_unary(std::forward<Expr>(expr), ops::logical_not{});
}

}  // namespace dpe
