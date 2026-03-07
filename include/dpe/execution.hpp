#pragma once

#include <algorithm>
#include <cstddef>
#include <future>
#include <iterator>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if __has_include(<execution>)
#include <execution>
#endif

#include "dpe/detail/thread_pool.hpp"

#if defined(__cpp_lib_execution) && (__cpp_lib_execution >= 201902L)
#define DPE_INTERNAL_HAS_STD_EXECUTION 1
#else
#define DPE_INTERNAL_HAS_STD_EXECUTION 0
#endif

namespace dpe {

namespace detail {

inline constexpr bool has_std_execution = DPE_INTERNAL_HAS_STD_EXECUTION != 0;

}  // namespace detail

template <typename Derived>
class execution_policy_base {
 public:
  template <typename Fn>
  void for_chunks(std::size_t total_rows, std::size_t grain_rows, Fn&& fn) const {
    derived().for_chunks_impl(total_rows, grain_rows, std::forward<Fn>(fn));
  }

  [[nodiscard]] auto chunk_count(std::size_t total_rows, std::size_t grain_rows) const
      -> std::size_t {
    return derived().chunk_count_impl(total_rows, grain_rows);
  }

  [[nodiscard]] auto suggested_grain_rows(std::size_t row_width_bytes = 64U) const -> std::size_t {
    return derived().suggested_grain_rows_impl(row_width_bytes);
  }

 protected:
  [[nodiscard]] constexpr auto derived() const noexcept -> const Derived& {
    return static_cast<const Derived&>(*this);
  }
};

struct sequenced_execution : execution_policy_base<sequenced_execution> {
  static constexpr bool is_parallel = false;
  static constexpr bool uses_std_execution = detail::has_std_execution;

  [[nodiscard]] constexpr auto chunk_count_impl(std::size_t total_rows, std::size_t) const
      -> std::size_t {
    return total_rows == 0U ? 0U : 1U;
  }

  template <typename Fn>
  void for_chunks_impl(std::size_t total_rows, std::size_t, Fn&& fn) const {
    if (total_rows != 0U) {
      fn(0U, total_rows, 0U);
    }
  }

  [[nodiscard]] constexpr auto suggested_grain_rows_impl(std::size_t row_width_bytes) const
      -> std::size_t {
    return std::max<std::size_t>(1U, (256U * 1024U) / std::max<std::size_t>(1U, row_width_bytes));
  }

  template <std::random_access_iterator Iterator, typename Fn>
  static void vectorized_for(Iterator begin, Iterator end, Fn&& fn) {
#if DPE_INTERNAL_HAS_STD_EXECUTION
    std::for_each(std::execution::unseq, begin, end, std::forward<Fn>(fn));
#else
    std::for_each(begin, end, std::forward<Fn>(fn));
#endif
  }
};

class parallel_execution : public execution_policy_base<parallel_execution> {
 public:
  explicit parallel_execution(std::size_t thread_count =
                                  std::max<std::size_t>(1U, std::thread::hardware_concurrency()),
                              std::size_t default_grain_rows = 1U << 16U)
      : default_grain_rows_{default_grain_rows},
        pool_{std::make_shared<detail::thread_pool>(thread_count)} {}

  static constexpr bool is_parallel = true;
  static constexpr bool uses_std_execution = detail::has_std_execution;

  [[nodiscard]] auto chunk_count_impl(std::size_t total_rows, std::size_t grain_rows) const
      -> std::size_t {
    if (total_rows == 0U) {
      return 0U;
    }

    const auto requested_grain = std::max<std::size_t>(1U, grain_rows);
    const auto requested_tasks = (total_rows + requested_grain - 1U) / requested_grain;
    const auto max_tasks = std::max<std::size_t>(1U, pool_->size() * 2U);
    return std::max<std::size_t>(1U, std::min(requested_tasks, max_tasks));
  }

  template <typename Fn>
  void for_chunks_impl(std::size_t total_rows, std::size_t grain_rows, Fn&& fn) const {
    if (total_rows == 0U) {
      return;
    }

    const auto task_count = chunk_count_impl(total_rows, grain_rows);
    const auto actual_grain = (total_rows + task_count - 1U) / task_count;

    std::vector<std::future<void>> futures;
    futures.reserve(task_count);
    auto shared_fn = std::make_shared<std::decay_t<Fn>>(std::forward<Fn>(fn));

    for (std::size_t task_index = 0U; task_index < task_count; ++task_index) {
      const auto begin = task_index * actual_grain;
      const auto end = std::min(begin + actual_grain, total_rows);

      futures.push_back(pool_->enqueue([begin, end, task_index, shared_fn]() {
        (*shared_fn)(begin, end, task_index);
      }));
    }

    for (auto& future : futures) {
      future.get();
    }
  }

  [[nodiscard]] auto suggested_grain_rows_impl(std::size_t row_width_bytes) const -> std::size_t {
    const auto target_rows =
        (512U * 1024U) / std::max<std::size_t>(1U, row_width_bytes);
    return std::max(default_grain_rows_, std::max<std::size_t>(1U, target_rows));
  }

  [[nodiscard]] auto concurrency() const noexcept -> std::size_t {
    return pool_->size();
  }

  template <std::random_access_iterator Iterator, typename Fn>
  static void vectorized_for(Iterator begin, Iterator end, Fn&& fn) {
#if DPE_INTERNAL_HAS_STD_EXECUTION
    std::for_each(std::execution::par_unseq, begin, end, std::forward<Fn>(fn));
#else
    std::for_each(begin, end, std::forward<Fn>(fn));
#endif
  }

 private:
  std::size_t default_grain_rows_;
  std::shared_ptr<detail::thread_pool> pool_;
};

}  // namespace dpe

#undef DPE_INTERNAL_HAS_STD_EXECUTION
