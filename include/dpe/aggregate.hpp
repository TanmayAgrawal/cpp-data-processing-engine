#pragma once

#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>

#include "dpe/concepts.hpp"
#include "dpe/fixed_string.hpp"
#include "dpe/schema.hpp"

namespace dpe {

template <fixed_string Alias>
struct count_aggregate {
  static constexpr auto alias = Alias;
  static constexpr auto has_input = false;
};

template <fixed_string Column, fixed_string Alias>
struct sum_aggregate {
  static constexpr auto column = Column;
  static constexpr auto alias = Alias;
  static constexpr auto has_input = true;
};

template <fixed_string Column, fixed_string Alias>
struct min_aggregate {
  static constexpr auto column = Column;
  static constexpr auto alias = Alias;
  static constexpr auto has_input = true;
};

template <fixed_string Column, fixed_string Alias>
struct max_aggregate {
  static constexpr auto column = Column;
  static constexpr auto alias = Alias;
  static constexpr auto has_input = true;
};

template <fixed_string Column, fixed_string Alias>
struct avg_aggregate {
  static constexpr auto column = Column;
  static constexpr auto alias = Alias;
  static constexpr auto has_input = true;
};

template <fixed_string Alias>
[[nodiscard]] constexpr auto count() -> count_aggregate<Alias> {
  return {};
}

template <fixed_string Alias>
[[nodiscard]] constexpr auto count(name_tag<Alias>) -> count_aggregate<Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto sum() -> sum_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto sum(name_tag<Column>, name_tag<Alias>)
    -> sum_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto min() -> min_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto min(name_tag<Column>, name_tag<Alias>)
    -> min_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto max() -> max_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto max(name_tag<Column>, name_tag<Alias>)
    -> max_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto avg() -> avg_aggregate<Column, Alias> {
  return {};
}

template <fixed_string Column, fixed_string Alias>
[[nodiscard]] constexpr auto avg(name_tag<Column>, name_tag<Alias>)
    -> avg_aggregate<Column, Alias> {
  return {};
}

template <typename T>
struct aggregate_output_type;

template <typename T>
struct aggregate_output_type_sum {
  using type = std::conditional_t<std::floating_point<bare_t<T>>, double, std::int64_t>;
};

template <typename T>
using aggregate_output_type_sum_t = typename aggregate_output_type_sum<T>::type;

template <typename Aggregate, typename Schema>
struct aggregate_result;

template <fixed_string Alias, typename Schema>
struct aggregate_result<count_aggregate<Alias>, Schema> {
  using type = std::int64_t;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_result<sum_aggregate<Column, Alias>, Schema> {
  using input_type = column_type_t<Column, Schema>;
  static_assert(numeric_like<input_type>, "sum requires a numeric input column.");
  using type = aggregate_output_type_sum_t<input_type>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_result<min_aggregate<Column, Alias>, Schema> {
  using input_type = column_type_t<Column, Schema>;
  static_assert(numeric_like<input_type>, "min requires a numeric input column.");
  using type = normalized_storage_type_t<input_type>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_result<max_aggregate<Column, Alias>, Schema> {
  using input_type = column_type_t<Column, Schema>;
  static_assert(numeric_like<input_type>, "max requires a numeric input column.");
  using type = normalized_storage_type_t<input_type>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_result<avg_aggregate<Column, Alias>, Schema> {
  using input_type = column_type_t<Column, Schema>;
  static_assert(numeric_like<input_type>, "avg requires a numeric input column.");
  using type = double;
};

template <typename Aggregate, typename Schema>
using aggregate_result_t = typename aggregate_result<Aggregate, Schema>::type;

template <typename Aggregate>
struct aggregate_name;

template <fixed_string Alias>
struct aggregate_name<count_aggregate<Alias>> {
  static constexpr auto value = Alias;
};

template <fixed_string Column, fixed_string Alias>
struct aggregate_name<sum_aggregate<Column, Alias>> {
  static constexpr auto value = Alias;
};

template <fixed_string Column, fixed_string Alias>
struct aggregate_name<min_aggregate<Column, Alias>> {
  static constexpr auto value = Alias;
};

template <fixed_string Column, fixed_string Alias>
struct aggregate_name<max_aggregate<Column, Alias>> {
  static constexpr auto value = Alias;
};

template <fixed_string Column, fixed_string Alias>
struct aggregate_name<avg_aggregate<Column, Alias>> {
  static constexpr auto value = Alias;
};

template <typename Aggregate>
inline constexpr auto aggregate_name_v = aggregate_name<Aggregate>::value;

namespace detail {

template <typename T>
struct count_state {
  std::int64_t value{0};

  constexpr void update() noexcept {
    ++value;
  }

  constexpr void merge(const count_state& other) noexcept {
    value += other.value;
  }

  [[nodiscard]] constexpr auto finalize() const noexcept -> std::int64_t {
    return value;
  }
};

template <typename T>
struct sum_state {
  using result_type = aggregate_output_type_sum_t<T>;
  result_type value{};

  constexpr void update(T input) noexcept {
    value += static_cast<result_type>(input);
  }

  constexpr void merge(const sum_state& other) noexcept {
    value += other.value;
  }

  [[nodiscard]] constexpr auto finalize() const noexcept -> result_type {
    return value;
  }
};

template <typename T>
struct min_state {
  using result_type = normalized_storage_type_t<T>;
  result_type value{};
  bool seen{false};

  constexpr void update(T input) noexcept {
    const auto normalized = static_cast<result_type>(input);
    if (!seen || normalized < value) {
      value = normalized;
      seen = true;
    }
  }

  constexpr void merge(const min_state& other) noexcept {
    if (other.seen) {
      update(other.value);
    }
  }

  [[nodiscard]] constexpr auto finalize() const noexcept -> result_type {
    return value;
  }
};

template <typename T>
struct max_state {
  using result_type = normalized_storage_type_t<T>;
  result_type value{};
  bool seen{false};

  constexpr void update(T input) noexcept {
    const auto normalized = static_cast<result_type>(input);
    if (!seen || normalized > value) {
      value = normalized;
      seen = true;
    }
  }

  constexpr void merge(const max_state& other) noexcept {
    if (other.seen) {
      update(other.value);
    }
  }

  [[nodiscard]] constexpr auto finalize() const noexcept -> result_type {
    return value;
  }
};

template <typename T>
struct avg_state {
  double sum{0.0};
  std::int64_t count{0};

  constexpr void update(T input) noexcept {
    sum += static_cast<double>(input);
    ++count;
  }

  constexpr void merge(const avg_state& other) noexcept {
    sum += other.sum;
    count += other.count;
  }

  [[nodiscard]] constexpr auto finalize() const noexcept -> double {
    return count == 0 ? 0.0 : (sum / static_cast<double>(count));
  }
};

template <typename Aggregate, typename Schema>
struct aggregate_state;

template <fixed_string Alias, typename Schema>
struct aggregate_state<count_aggregate<Alias>, Schema> {
  using type = count_state<std::int64_t>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_state<sum_aggregate<Column, Alias>, Schema> {
  using type = sum_state<column_type_t<Column, Schema>>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_state<min_aggregate<Column, Alias>, Schema> {
  using type = min_state<column_type_t<Column, Schema>>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_state<max_aggregate<Column, Alias>, Schema> {
  using type = max_state<column_type_t<Column, Schema>>;
};

template <fixed_string Column, fixed_string Alias, typename Schema>
struct aggregate_state<avg_aggregate<Column, Alias>, Schema> {
  using type = avg_state<column_type_t<Column, Schema>>;
};

template <typename Aggregate, typename Schema>
using aggregate_state_t = typename aggregate_state<Aggregate, Schema>::type;

}  // namespace detail

}  // namespace dpe
