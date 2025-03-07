#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "dpe/aggregate.hpp"
#include "dpe/batch.hpp"
#include "dpe/detail/aligned_allocator.hpp"
#include "dpe/detail/group_map.hpp"
#include "dpe/execution.hpp"
#include "dpe/expression.hpp"
#include "dpe/schema.hpp"
#include "dpe/storage.hpp"

namespace dpe {

template <typename Schema>
class materialized_table;

template <typename Schema, typename StoragePolicy>
class table_snapshot;

template <typename RootSchema, typename StoragePolicy, typename ExecPolicy, typename Bindings,
          typename FilterExpr>
class relation;

template <typename RootSchema, typename StoragePolicy, typename ExecPolicy, typename Bindings,
          typename FilterExpr, fixed_string... Keys>
class grouped_relation;

namespace detail {

template <typename T>
struct materialized_column_type {
  using type = std::vector<T, aligned_allocator<T>>;
};

template <>
struct materialized_column_type<std::string> {
  using type = std::vector<std::string>;
};

template <typename T>
using materialized_column_t = typename materialized_column_type<T>::type;

template <typename T>
inline constexpr bool can_vectorize_materialization_v =
    std::is_arithmetic_v<T> && !std::same_as<T, bool>;

template <fixed_string Name, typename Expr>
struct binding {
  using expr_type = Expr;
  static constexpr auto name = Name;

  Expr expr;
};

template <fixed_string Name, typename First, typename... Rest>
consteval auto binding_index_impl() -> std::size_t {
  if constexpr (First::name == Name) {
    return 0U;
  } else {
    static_assert(sizeof...(Rest) > 0U, "Requested projection is not available in this relation.");
    return 1U + binding_index_impl<Name, Rest...>();
  }
}

template <typename... Bindings>
struct binding_pack {
  std::tuple<Bindings...> values;

  template <fixed_string Name>
  static consteval auto contains() -> bool {
    return ((Bindings::name == Name) || ...);
  }

  template <fixed_string Name>
  static consteval auto index_of() -> std::size_t {
    static_assert(contains<Name>(), "Requested projection is not available in this relation.");
    return binding_index_impl<Name, Bindings...>();
  }

  template <fixed_string Name>
  [[nodiscard]] constexpr auto expr() const -> const auto& {
    return std::get<index_of<Name>()>(values).expr;
  }

  template <fixed_string Name>
  [[nodiscard]] constexpr auto expr(name_tag<Name>) const -> const auto& {
    return expr<Name>();
  }
};

template <typename RootSchema, typename BindingPack>
struct binding_pack_schema;

template <typename RootSchema, typename... Bindings>
struct binding_pack_schema<RootSchema, binding_pack<Bindings...>> {
  // The current relation schema is derived from bound expressions instead of materialized columns.
  // That lets us carry a compile-time, type-safe "virtual schema" through lazy query chains.
  static_assert(detail::unique_names_v<Bindings::name...>,
                "Projection aliases must be unique at compile time.");

  using type = schema<
      field<Bindings::name,
            normalized_storage_type_t<expression_value_t<typename Bindings::expr_type, RootSchema>>>...>;

  static_assert((materializable_expression<
                 expression_value_t<typename Bindings::expr_type, RootSchema>> &&
                 ...),
                "Projected expressions must materialize to supported engine types.");
};

template <typename RootSchema, typename BindingPack>
using binding_pack_schema_t = typename binding_pack_schema<RootSchema, BindingPack>::type;

template <typename Schema, std::size_t... I>
constexpr auto make_identity_bindings_impl(std::index_sequence<I...>) {
  return binding_pack<
      binding<schema_field_t<I, Schema>::name, column_ref<schema_field_t<I, Schema>::name>>...>{
      std::tuple{binding<schema_field_t<I, Schema>::name,
                         column_ref<schema_field_t<I, Schema>::name>>{
          col<schema_field_t<I, Schema>::name>()}...}};
}

template <typename Schema>
constexpr auto make_identity_bindings() {
  return make_identity_bindings_impl<Schema>(
      std::make_index_sequence<Schema::column_count>{});
}

template <typename Schema>
using identity_bindings_t = decltype(make_identity_bindings<Schema>());

struct always_true_expr : expression_tag {
  template <typename Schema>
  using result_type = bool;

  template <typename Snapshot>
  [[nodiscard]] constexpr auto eval(const Snapshot&, std::size_t) const noexcept -> bool {
    return true;
  }
};

template <typename Bindings, fixed_string Name>
constexpr auto bind_expression(const column_ref<Name>&, const Bindings& bindings) {
  return bindings.template expr<Name>();
}

template <typename Bindings, typename T>
constexpr auto bind_expression(const literal<T>& expr, const Bindings&) {
  return expr;
}

template <typename Bindings, typename Op, typename Expr>
constexpr auto bind_expression(const unary_expr<Op, Expr>& expr, const Bindings& bindings) {
  auto bound = bind_expression(expr.expr, bindings);
  return unary_expr<Op, bare_t<decltype(bound)>>{.expr = bound};
}

template <typename Bindings, typename Op, typename Left, typename Right>
constexpr auto bind_expression(const binary_expr<Op, Left, Right>& expr,
                               const Bindings& bindings) {
  auto lhs = bind_expression(expr.lhs, bindings);
  auto rhs = bind_expression(expr.rhs, bindings);
  return binary_expr<Op, bare_t<decltype(lhs)>, bare_t<decltype(rhs)>>{
      .lhs = lhs,
      .rhs = rhs};
}

template <fixed_string Alias, typename Expr, typename Bindings>
constexpr auto bind_named_expression(const named_expression<Alias, Expr>& expr,
                                     const Bindings& bindings) {
  auto bound = bind_expression(expr.expr, bindings);
  return binding<Alias, bare_t<decltype(bound)>>{.expr = bound};
}

template <typename T>
struct is_named_expression : std::false_type {};

template <fixed_string Alias, typename Expr>
struct is_named_expression<named_expression<Alias, Expr>> : std::true_type {};

template <typename T>
inline constexpr bool is_named_expression_v = is_named_expression<bare_t<T>>::value;

template <typename Target, typename Value>
auto materialize_value(Value&& value) -> Target {
  if constexpr (std::same_as<Target, std::string>) {
    return std::string{std::forward<Value>(value)};
  } else {
    return static_cast<Target>(std::forward<Value>(value));
  }
}

template <typename Tuple, std::size_t... I>
constexpr void resize_tuple_columns(Tuple& tuple, std::size_t size, std::index_sequence<I...>) {
  (std::get<I>(tuple).resize(size), ...);
}

template <typename Snapshot, typename FilterExpr, typename ExecPolicy>
auto build_selection_vector(const Snapshot& snapshot, const FilterExpr& filter,
                            const ExecPolicy& exec_policy) -> std::vector<std::size_t> {
  const auto row_count = snapshot.row_count();
  const auto grain_rows = exec_policy.suggested_grain_rows(64U);

  if constexpr (!ExecPolicy::is_parallel) {
    std::vector<std::size_t> selected;
    selected.reserve(row_count / 2U);
    for (std::size_t row = 0U; row < row_count; ++row) {
      if (filter.eval(snapshot, row)) {
        selected.push_back(row);
      }
    }
    return selected;
  } else {
    const auto task_count = row_count == 0U ? 0U : ((row_count + grain_rows - 1U) / grain_rows);
    std::vector<std::vector<std::size_t>> locals(task_count);

    exec_policy.for_chunks(row_count, grain_rows,
                           [&](std::size_t begin, std::size_t end, std::size_t task) {
                             auto& local = locals[task];
                             local.reserve(end - begin);
                             for (std::size_t row = begin; row < end; ++row) {
                               if (filter.eval(snapshot, row)) {
                                 local.push_back(row);
                               }
                             }
                           });

    std::size_t total_selected = 0U;
    for (const auto& local : locals) {
      total_selected += local.size();
    }

    std::vector<std::size_t> selected;
    selected.reserve(total_selected);
    for (auto& local : locals) {
      selected.insert(selected.end(), local.begin(), local.end());
    }
    return selected;
  }
}

template <typename OutputSchema, typename Snapshot, typename Bindings, typename ExecPolicy,
          std::size_t I>
void fill_output_column(materialized_table<OutputSchema>& result, const Snapshot& snapshot,
                        const Bindings& bindings, const ExecPolicy&,
                        const std::vector<std::size_t>* selection) {
  using field_type = schema_field_t<I, OutputSchema>;
  using target_type = typename field_type::value_type;

  auto& column = result.template mutable_column_by_index<I>();
  if constexpr (can_vectorize_materialization_v<target_type>) {
    auto* base = column.data();
    ExecPolicy::vectorized_for(column.begin(), column.end(), [&](auto& cell) {
      const auto logical_row = static_cast<std::size_t>(&cell - base);
      const auto source_row = selection == nullptr ? logical_row : (*selection)[logical_row];
      cell = materialize_value<target_type>(
          bindings.template expr<field_type::name>().eval(snapshot, source_row));
    });
  } else {
    for (std::size_t logical_row = 0U; logical_row < column.size(); ++logical_row) {
      const auto source_row = selection == nullptr ? logical_row : (*selection)[logical_row];
      column[logical_row] = materialize_value<target_type>(
          bindings.template expr<field_type::name>().eval(snapshot, source_row));
    }
  }
}

template <typename OutputSchema, typename Snapshot, typename Bindings, typename ExecPolicy,
          std::size_t... I>
void fill_output_columns(materialized_table<OutputSchema>& result, const Snapshot& snapshot,
                         const Bindings& bindings, const ExecPolicy& exec_policy,
                         const std::vector<std::size_t>* selection,
                         std::index_sequence<I...>) {
  (fill_output_column<OutputSchema, Snapshot, Bindings, ExecPolicy, I>(
       result, snapshot, bindings, exec_policy, selection),
   ...);
}

template <typename OutputSchema, typename Snapshot, typename Bindings, typename ExecPolicy>
auto materialize_projection(const Snapshot& snapshot, const Bindings& bindings,
                            const ExecPolicy& exec_policy,
                            const std::vector<std::size_t>* selection)
    -> materialized_table<OutputSchema> {
  const auto output_rows = selection == nullptr ? snapshot.row_count() : selection->size();
  materialized_table<OutputSchema> result{output_rows};
  fill_output_columns(result, snapshot, bindings, exec_policy, selection,
                      std::make_index_sequence<OutputSchema::column_count>{});
  return result;
}

template <typename Aggregate, typename CurrentSchema, typename RootSnapshot, typename Bindings,
          typename State>
void update_state(State& state, const RootSnapshot& snapshot, const Bindings& bindings,
                  std::size_t row) {
  if constexpr (Aggregate::has_input) {
    state.update(bindings.template expr<Aggregate::column>().eval(snapshot, row));
  } else {
    state.update();
  }
}

template <typename CurrentSchema, typename RootSnapshot, typename Bindings, typename StateTuple,
          typename... Aggregates, std::size_t... I>
void update_state_tuple(StateTuple& states, const RootSnapshot& snapshot, const Bindings& bindings,
                        std::size_t row, std::index_sequence<I...>) {
  (update_state<Aggregates, CurrentSchema>(std::get<I>(states), snapshot, bindings, row), ...);
}

template <typename StateTuple, std::size_t... I>
void merge_state_tuple(StateTuple& destination, const StateTuple& source,
                       std::index_sequence<I...>) {
  (std::get<I>(destination).merge(std::get<I>(source)), ...);
}

template <typename CurrentSchema, typename KeyList, typename... Aggregates>
struct aggregate_output_schema;

template <typename CurrentSchema, fixed_string... Keys, typename... Aggregates>
struct aggregate_output_schema<CurrentSchema, fixed_string_list<Keys...>, Aggregates...> {
  static_assert(detail::unique_names_v<Keys..., aggregate_name_v<Aggregates>...>,
                "Group-by keys and aggregate aliases must be unique at compile time.");

  using type = schema<field<Keys, column_type_t<Keys, CurrentSchema>>...,
                      field<aggregate_name_v<Aggregates>,
                            aggregate_result_t<Aggregates, CurrentSchema>>...>;
};

template <typename CurrentSchema, typename KeyList, typename... Aggregates>
using aggregate_output_schema_t =
    typename aggregate_output_schema<CurrentSchema, KeyList, Aggregates...>::type;

template <fixed_string... Keys, typename Snapshot, typename Bindings>
auto make_group_key(const Snapshot& snapshot, const Bindings& bindings, std::size_t row) {
  return std::tuple{bindings.template expr<Keys>().eval(snapshot, row)...};
}

template <typename OutputSchema, typename KeyTuple, typename StateTuple, std::size_t... KeyI,
          std::size_t... AggI>
void write_group_row(materialized_table<OutputSchema>& result, std::size_t row,
                     const KeyTuple& key_tuple, const StateTuple& states,
                     std::index_sequence<KeyI...>, std::index_sequence<AggI...>) {
  ((result.template mutable_column_by_index<KeyI>()[row] =
        materialize_value<typename schema_field_t<KeyI, OutputSchema>::value_type>(
            std::get<KeyI>(key_tuple))),
   ...);

  ((result.template mutable_column_by_index<sizeof...(KeyI) + AggI>()[row] =
        materialize_value<
            typename schema_field_t<sizeof...(KeyI) + AggI, OutputSchema>::value_type>(
            std::get<AggI>(states).finalize())),
   ...);
}

template <typename OutputSchema, typename StateTuple, std::size_t... I>
void write_scalar_aggregate_row(materialized_table<OutputSchema>& result, const StateTuple& states,
                                std::index_sequence<I...>) {
  ((result.template mutable_column_by_index<I>()[0] =
        materialize_value<typename schema_field_t<I, OutputSchema>::value_type>(
            std::get<I>(states).finalize())),
   ...);
}

template <typename Tuple, typename Fn, std::size_t... I>
void for_each_tuple_index(Tuple&& tuple, Fn&& fn, std::index_sequence<I...>) {
  (fn.template operator()<I>(std::get<I>(std::forward<Tuple>(tuple))), ...);
}

}  // namespace detail

template <typename... Fields>
class materialized_table<schema<Fields...>> {
 public:
  using schema_type = schema<Fields...>;
  using columns_type = std::tuple<detail::materialized_column_t<typename Fields::value_type>...>;

  explicit materialized_table(std::size_t row_count = 0U) {
    resize(row_count);
  }

  void resize(std::size_t row_count) {
    row_count_ = row_count;
    detail::resize_tuple_columns(columns_, row_count, std::index_sequence_for<Fields...>{});
  }

  [[nodiscard]] auto row_count() const noexcept -> std::size_t {
    return row_count_;
  }

  template <std::size_t I>
  [[nodiscard]] auto column_by_index() const -> const auto& {
    return std::get<I>(columns_);
  }

  template <std::size_t I>
  [[nodiscard]] auto mutable_column_by_index() -> auto& {
    return std::get<I>(columns_);
  }

  template <fixed_string Name>
  [[nodiscard]] auto column() const -> const auto& {
    return std::get<schema_type::template index_of<Name>()>(columns_);
  }

  template <fixed_string Name>
  [[nodiscard]] auto column(name_tag<Name>) const -> const auto& {
    return column<Name>();
  }

  template <fixed_string Name>
  [[nodiscard]] auto value(std::size_t row) const
      -> expression_view_type_t<column_type_t<Name, schema_type>> {
    const auto& column_ref = column<Name>();
    if constexpr (std::same_as<column_type_t<Name, schema_type>, std::string>) {
      return column_ref[row];
    } else {
      return column_ref[row];
    }
  }

  template <fixed_string Name>
  [[nodiscard]] auto value(name_tag<Name>, std::size_t row) const
      -> expression_view_type_t<column_type_t<Name, schema_type>> {
    return value<Name>(row);
  }

 private:
  columns_type columns_{};
  std::size_t row_count_{0U};
};

template <typename StoragePolicy, typename... Fields>
class table_snapshot<schema<Fields...>, StoragePolicy> {
 public:
  using schema_type = schema<Fields...>;
  using storage_policy_type = StoragePolicy;
  using columns_type =
      std::tuple<std::shared_ptr<const typename StoragePolicy::template column<typename Fields::value_type>>...>;

  table_snapshot(columns_type columns, std::size_t row_count)
      : columns_{std::move(columns)}, row_count_{row_count} {}

  [[nodiscard]] auto row_count() const noexcept -> std::size_t {
    return row_count_;
  }

  template <std::size_t I>
  [[nodiscard]] auto column_ptr_by_index() const -> const auto& {
    return std::get<I>(columns_);
  }

  template <fixed_string Name>
  [[nodiscard]] auto value(std::size_t row) const
      -> expression_view_type_t<column_type_t<Name, schema_type>> {
    const auto& column_ptr = std::get<schema_type::template index_of<Name>()>(columns_);
    return column_ptr->at(row);
  }

  template <fixed_string Name>
  [[nodiscard]] auto value(name_tag<Name>, std::size_t row) const
      -> expression_view_type_t<column_type_t<Name, schema_type>> {
    return value<Name>(row);
  }

 private:
  columns_type columns_;
  std::size_t row_count_{0U};
};

template <typename RootSchema, typename StoragePolicy, typename ExecPolicy, typename Bindings,
          typename FilterExpr>
class relation {
 public:
  using root_schema_type = RootSchema;
  using current_schema_type = detail::binding_pack_schema_t<RootSchema, Bindings>;
  using snapshot_type = table_snapshot<RootSchema, StoragePolicy>;

  relation(std::shared_ptr<const snapshot_type> snapshot, Bindings bindings, FilterExpr filter,
           ExecPolicy exec_policy)
      : snapshot_{std::move(snapshot)},
        bindings_{std::move(bindings)},
        filter_{std::move(filter)},
        exec_policy_{std::move(exec_policy)} {}

  template <typename Expr>
    requires boolean_expression_for<Expr, current_schema_type>
  [[nodiscard]] auto filter(Expr&& expr) const {
    auto bound = detail::bind_expression(detail::as_expression(std::forward<Expr>(expr)), bindings_);
    if constexpr (std::same_as<FilterExpr, detail::always_true_expr>) {
      return relation<RootSchema, StoragePolicy, ExecPolicy, Bindings,
                      bare_t<decltype(bound)>>{snapshot_, bindings_, bound, exec_policy_};
    } else {
      auto combined = filter_ && bound;
      return relation<RootSchema, StoragePolicy, ExecPolicy, Bindings,
                      bare_t<decltype(combined)>>{snapshot_, bindings_, combined, exec_policy_};
    }
  }

  template <typename... NamedExprs>
    requires(sizeof...(NamedExprs) > 0U && (detail::is_named_expression_v<NamedExprs> && ...) &&
             (projection_expression_for<typename bare_t<NamedExprs>::expr_type,
                                        current_schema_type> &&
              ...))
  [[nodiscard]] auto select(NamedExprs&&... expressions) const {
    using new_bindings_type =
        detail::binding_pack<bare_t<decltype(detail::bind_named_expression(
            std::forward<NamedExprs>(expressions), bindings_))>...>;

    auto new_bindings = new_bindings_type{
        std::tuple{detail::bind_named_expression(std::forward<NamedExprs>(expressions), bindings_)...}};

    return relation<RootSchema, StoragePolicy, ExecPolicy, new_bindings_type, FilterExpr>{
        snapshot_, std::move(new_bindings), filter_, exec_policy_};
  }

  template <fixed_string... Keys>
    requires(sizeof...(Keys) > 0U && (schema_contains_v<Keys, current_schema_type> && ...))
  [[nodiscard]] auto groupBy() const {
    return grouped_relation<RootSchema, StoragePolicy, ExecPolicy, Bindings, FilterExpr, Keys...>{
        snapshot_, bindings_, filter_, exec_policy_};
  }

  template <fixed_string... Keys>
    requires(sizeof...(Keys) > 0U && (schema_contains_v<Keys, current_schema_type> && ...))
  [[nodiscard]] auto groupBy(name_tag<Keys>...) const {
    return groupBy<Keys...>();
  }

  [[nodiscard]] auto evaluate() const -> materialized_table<current_schema_type> {
    if constexpr (std::same_as<FilterExpr, detail::always_true_expr>) {
      return detail::materialize_projection<current_schema_type>(*snapshot_, bindings_,
                                                                 exec_policy_, nullptr);
    } else {
      auto selection = detail::build_selection_vector(*snapshot_, filter_, exec_policy_);
      return detail::materialize_projection<current_schema_type>(*snapshot_, bindings_,
                                                                 exec_policy_, &selection);
    }
  }

  template <typename... Aggregates>
    requires(sizeof...(Aggregates) > 0U)
  [[nodiscard]] auto aggregate(Aggregates...) const
      -> materialized_table<
          detail::aggregate_output_schema_t<current_schema_type, fixed_string_list<>,
                                            Aggregates...>> {
    using output_schema =
        detail::aggregate_output_schema_t<current_schema_type, fixed_string_list<>,
                                          Aggregates...>;
    using state_tuple = std::tuple<detail::aggregate_state_t<bare_t<Aggregates>, current_schema_type>...>;

    state_tuple merged_states{};
    const auto row_count = snapshot_->row_count();
    const auto grain_rows = exec_policy_.suggested_grain_rows(64U);

    if constexpr (!ExecPolicy::is_parallel) {
      for (std::size_t row = 0U; row < row_count; ++row) {
        if (filter_.eval(*snapshot_, row)) {
          detail::update_state_tuple<current_schema_type, snapshot_type, Bindings, state_tuple,
                                     bare_t<Aggregates>...>(
              merged_states, *snapshot_, bindings_, row,
              std::index_sequence_for<Aggregates...>{});
        }
      }
    } else {
      const auto task_count = row_count == 0U ? 0U : ((row_count + grain_rows - 1U) / grain_rows);
      std::vector<state_tuple> locals(task_count);

      exec_policy_.for_chunks(
          row_count, grain_rows, [&](std::size_t begin, std::size_t end, std::size_t task) {
            auto& states = locals[task];
            for (std::size_t row = begin; row < end; ++row) {
              if (filter_.eval(*snapshot_, row)) {
                detail::update_state_tuple<current_schema_type, snapshot_type, Bindings,
                                           state_tuple, bare_t<Aggregates>...>(
                    states, *snapshot_, bindings_, row,
                    std::index_sequence_for<Aggregates...>{});
              }
            }
          });

      for (const auto& local : locals) {
        detail::merge_state_tuple(merged_states, local,
                                  std::index_sequence_for<Aggregates...>{});
      }
    }

    materialized_table<output_schema> result{1U};
    detail::write_scalar_aggregate_row(result, merged_states,
                                       std::index_sequence_for<Aggregates...>{});
    return result;
  }

 private:
  std::shared_ptr<const snapshot_type> snapshot_;
  Bindings bindings_;
  FilterExpr filter_;
  ExecPolicy exec_policy_;
};

template <typename RootSchema, typename StoragePolicy, typename ExecPolicy, typename Bindings,
          typename FilterExpr, fixed_string... Keys>
class grouped_relation {
 public:
  using root_schema_type = RootSchema;
  using current_schema_type = detail::binding_pack_schema_t<RootSchema, Bindings>;
  using snapshot_type = table_snapshot<RootSchema, StoragePolicy>;

  grouped_relation(std::shared_ptr<const snapshot_type> snapshot, Bindings bindings,
                   FilterExpr filter, ExecPolicy exec_policy)
      : snapshot_{std::move(snapshot)},
        bindings_{std::move(bindings)},
        filter_{std::move(filter)},
        exec_policy_{std::move(exec_policy)} {}

  template <typename... Aggregates>
    requires(sizeof...(Aggregates) > 0U)
  [[nodiscard]] auto aggregate(Aggregates...) const
      -> materialized_table<
          detail::aggregate_output_schema_t<current_schema_type, fixed_string_list<Keys...>,
                                            Aggregates...>> {
    using output_schema =
        detail::aggregate_output_schema_t<current_schema_type, fixed_string_list<Keys...>,
                                          Aggregates...>;
    using key_type = decltype(detail::make_group_key<Keys...>(*snapshot_, bindings_, 0U));
    using state_tuple =
        std::tuple<detail::aggregate_state_t<bare_t<Aggregates>, current_schema_type>...>;
    using map_type = detail::open_addressing_table<key_type, state_tuple>;

    const auto row_count = snapshot_->row_count();
    const auto grain_rows = exec_policy_.suggested_grain_rows(64U);
    map_type merged_map{std::max<std::size_t>(16U, row_count / 4U + 1U)};

    if constexpr (!ExecPolicy::is_parallel) {
      for (std::size_t row = 0U; row < row_count; ++row) {
        if (!filter_.eval(*snapshot_, row)) {
          continue;
        }

        auto key = detail::make_group_key<Keys...>(*snapshot_, bindings_, row);
        auto& states = merged_map.find_or_emplace(key);
        detail::update_state_tuple<current_schema_type, snapshot_type, Bindings, state_tuple,
                                   bare_t<Aggregates>...>(
            states, *snapshot_, bindings_, row, std::index_sequence_for<Aggregates...>{});
      }
    } else {
      const auto task_count = row_count == 0U ? 0U : ((row_count + grain_rows - 1U) / grain_rows);
      std::vector<map_type> locals;
      locals.reserve(task_count);
      for (std::size_t task = 0U; task < task_count; ++task) {
        locals.emplace_back(std::max<std::size_t>(16U, grain_rows / 4U + 1U));
      }

      exec_policy_.for_chunks(
          row_count, grain_rows, [&](std::size_t begin, std::size_t end, std::size_t task) {
            auto& local_map = locals[task];
            for (std::size_t row = begin; row < end; ++row) {
              if (!filter_.eval(*snapshot_, row)) {
                continue;
              }

              auto key = detail::make_group_key<Keys...>(*snapshot_, bindings_, row);
              auto& states = local_map.find_or_emplace(key);
              detail::update_state_tuple<current_schema_type, snapshot_type, Bindings,
                                         state_tuple, bare_t<Aggregates>...>(
                  states, *snapshot_, bindings_, row,
                  std::index_sequence_for<Aggregates...>{});
            }
          });

      for (const auto& local : locals) {
        local.for_each_occupied([&](const auto& key, const auto& states) {
          auto& merged_states = merged_map.find_or_emplace(key);
          detail::merge_state_tuple(merged_states, states,
                                    std::index_sequence_for<Aggregates...>{});
        });
      }
    }

    materialized_table<output_schema> result{merged_map.size()};
    std::size_t row_index = 0U;
    merged_map.for_each_occupied([&](const auto& key, const auto& states) {
      detail::write_group_row<output_schema>(
          result, row_index++, key, states, std::make_index_sequence<sizeof...(Keys)>{},
          std::index_sequence_for<Aggregates...>{});
    });

    return result;
  }

 private:
  std::shared_ptr<const snapshot_type> snapshot_;
  Bindings bindings_;
  FilterExpr filter_;
  ExecPolicy exec_policy_;
};

template <typename Schema, typename StoragePolicy = heap_storage_policy,
          typename ExecPolicy = sequenced_execution>
class table;

template <typename StoragePolicy, typename ExecPolicy, typename... Fields>
class table<schema<Fields...>, StoragePolicy, ExecPolicy> {
 public:
  using schema_type = schema<Fields...>;
  using snapshot_type = table_snapshot<schema_type, StoragePolicy>;

  explicit table(StoragePolicy storage_policy = {}, ExecPolicy exec_policy = {})
      : storage_policy_{std::move(storage_policy)}, exec_policy_{std::move(exec_policy)} {
    auto initial = make_empty_snapshot(std::index_sequence_for<Fields...>{});
    std::atomic_store(&snapshot_, std::const_pointer_cast<const snapshot_type>(initial));
  }

  void appendBatch(const batch<schema_type>& input) {
    [[maybe_unused]] auto locks = acquire_write_locks(std::index_sequence_for<Fields...>{});
    auto current = snapshot();
    auto next_columns = clone_columns(*current, input, std::index_sequence_for<Fields...>{});
    auto next =
        std::make_shared<snapshot_type>(std::move(next_columns), current->row_count() + input.row_count);
    std::atomic_store(&snapshot_, std::const_pointer_cast<const snapshot_type>(next));
  }

  [[nodiscard]] auto snapshot() const -> std::shared_ptr<const snapshot_type> {
    return std::atomic_load(&snapshot_);
  }

  [[nodiscard]] auto query() const {
    return relation<schema_type, StoragePolicy, ExecPolicy, detail::identity_bindings_t<schema_type>,
                    detail::always_true_expr>{snapshot(), detail::make_identity_bindings<schema_type>(),
                                              detail::always_true_expr{}, exec_policy_};
  }

  template <typename Expr>
  [[nodiscard]] auto filter(Expr&& expr) const {
    return query().filter(std::forward<Expr>(expr));
  }

  template <typename... NamedExprs>
  [[nodiscard]] auto select(NamedExprs&&... expressions) const {
    return query().select(std::forward<NamedExprs>(expressions)...);
  }

  template <fixed_string... Keys>
  [[nodiscard]] auto groupBy() const {
    return query().template groupBy<Keys...>();
  }

  template <fixed_string... Keys>
  [[nodiscard]] auto groupBy(name_tag<Keys>... keys) const {
    return query().groupBy(keys...);
  }

  template <typename... Aggregates>
  [[nodiscard]] auto aggregate(Aggregates... aggregates) const {
    return query().aggregate(aggregates...);
  }

  [[nodiscard]] auto evaluate() const {
    return query().evaluate();
  }

 private:
  using mutex_tuple_type = std::tuple<std::conditional_t<true, std::shared_mutex, Fields>...>;

  template <std::size_t... I>
  auto make_empty_snapshot(std::index_sequence<I...>) -> std::shared_ptr<snapshot_type> {
    return std::make_shared<snapshot_type>(
        typename snapshot_type::columns_type{
            storage_policy_.template make_empty<typename schema_field_t<I, schema_type>::value_type>(I)...},
        0U);
  }

  template <std::size_t... I>
  auto acquire_write_locks(std::index_sequence<I...>) const {
    // Writers always lock in schema order so multiple concurrent appenders cannot deadlock.
    return std::tuple{std::unique_lock<std::shared_mutex>{std::get<I>(column_mutexes_)}...};
  }

  template <std::size_t... I>
  auto clone_columns(const snapshot_type& current, const batch<schema_type>& input,
                     std::index_sequence<I...>) const -> typename snapshot_type::columns_type {
    return typename snapshot_type::columns_type{
        storage_policy_.template clone_append<typename schema_field_t<I, schema_type>::value_type>(
            current.template column_ptr_by_index<I>(), std::get<I>(input.columns), I)...};
  }

  mutable mutex_tuple_type column_mutexes_{};
  StoragePolicy storage_policy_;
  ExecPolicy exec_policy_;
  std::shared_ptr<const snapshot_type> snapshot_;
};

}  // namespace dpe
