#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "dpe/batch.hpp"
#include "dpe/concepts.hpp"
#include "dpe/detail/aligned_allocator.hpp"
#include "dpe/detail/mmap_region.hpp"

namespace dpe {

template <typename Derived>
class storage_policy_base {
 public:
  template <typename T>
  auto make_empty(std::size_t column_index) const {
    return derived().template make_empty_impl<T>(column_index);
  }

  template <typename T, typename Current>
  auto clone_append(Current current, batch_column_view<T> batch, std::size_t column_index) const {
    return derived().clone_append_impl(std::move(current), batch, column_index);
  }

 protected:
  [[nodiscard]] constexpr auto derived() const noexcept -> const Derived& {
    return static_cast<const Derived&>(*this);
  }
};

template <typename T>
class heap_column {
 public:
  using value_type = T;
  using storage_type = std::vector<T, detail::aligned_allocator<T>>;

  heap_column() = default;

  explicit heap_column(storage_type values) : values_{std::move(values)} {}

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return values_.size();
  }

  [[nodiscard]] auto data() const noexcept -> const T* {
    return values_.data();
  }

  [[nodiscard]] auto at(std::size_t row) const noexcept -> T {
    return values_[row];
  }

  [[nodiscard]] auto values() const noexcept -> std::span<const T> {
    return {values_.data(), values_.size()};
  }

  static auto append(const heap_column* current, batch_column_view<T> batch)
      -> std::shared_ptr<heap_column> {
    storage_type values;
    const auto existing_size = current == nullptr ? 0U : current->size();
    values.reserve(existing_size + batch.size());

    if (current != nullptr) {
      values.insert(values.end(), current->values_.begin(), current->values_.end());
    }

    values.insert(values.end(), batch.values.begin(), batch.values.end());
    return std::make_shared<heap_column>(std::move(values));
  }

 private:
  storage_type values_{};
};

template <>
class heap_column<std::string> {
 public:
  using value_type = std::string;

  heap_column() {
    offsets_.push_back(0U);
  }

  heap_column(std::vector<std::size_t, detail::aligned_allocator<std::size_t>> offsets,
              std::vector<char, detail::aligned_allocator<char>> chars)
      : offsets_{std::move(offsets)}, chars_{std::move(chars)} {}

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return offsets_.empty() ? 0U : offsets_.size() - 1U;
  }

  [[nodiscard]] auto chars_size() const noexcept -> std::size_t {
    return chars_.size();
  }

  [[nodiscard]] auto offsets() const noexcept -> std::span<const std::size_t> {
    return {offsets_.data(), offsets_.size()};
  }

  [[nodiscard]] auto chars() const noexcept -> std::span<const char> {
    return {chars_.data(), chars_.size()};
  }

  [[nodiscard]] auto at(std::size_t row) const noexcept -> std::string_view {
    const auto begin = offsets_[row];
    const auto end = offsets_[row + 1U];
    return {chars_.data() + begin, end - begin};
  }

  static auto append(const heap_column* current, batch_column_view<std::string> batch)
      -> std::shared_ptr<heap_column> {
    std::vector<std::size_t, detail::aligned_allocator<std::size_t>> offsets;
    std::vector<char, detail::aligned_allocator<char>> chars;

    if (current != nullptr) {
      offsets = current->offsets_;
      chars = current->chars_;
    } else {
      offsets.push_back(0U);
    }

    offsets.reserve(offsets.size() + batch.size());
    const auto additional_bytes =
        std::transform_reduce(batch.values.begin(), batch.values.end(), std::size_t{0U},
                              std::plus<>{},
                              [](std::string_view value) { return value.size(); });
    chars.reserve(chars.size() + additional_bytes);

    for (const auto value : batch.values) {
      chars.insert(chars.end(), value.begin(), value.end());
      offsets.push_back(chars.size());
    }

    return std::make_shared<heap_column>(std::move(offsets), std::move(chars));
  }

 private:
  std::vector<std::size_t, detail::aligned_allocator<std::size_t>> offsets_{};
  std::vector<char, detail::aligned_allocator<char>> chars_{};
};

template <typename T>
class mmap_column {
 public:
  using value_type = T;

  mmap_column() = default;

  mmap_column(detail::mapped_region region, std::size_t size)
      : region_{std::move(region)}, size_{size} {}

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return size_;
  }

  [[nodiscard]] auto data() const noexcept -> const T* {
    return reinterpret_cast<const T*>(region_.data());
  }

  [[nodiscard]] auto at(std::size_t row) const noexcept -> T {
    return data()[row];
  }

 private:
  detail::mapped_region region_{};
  std::size_t size_{0U};

  friend class mmap_storage_policy;
};

template <>
class mmap_column<std::string> {
 public:
  using value_type = std::string;

  mmap_column() = default;

  mmap_column(detail::mapped_region offsets_region, detail::mapped_region chars_region,
              std::size_t row_count, std::size_t chars_size)
      : offsets_region_{std::move(offsets_region)},
        chars_region_{std::move(chars_region)},
        row_count_{row_count},
        chars_size_{chars_size} {}

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return row_count_;
  }

  [[nodiscard]] auto chars_size() const noexcept -> std::size_t {
    return chars_size_;
  }

  [[nodiscard]] auto offsets() const noexcept -> const std::size_t* {
    return reinterpret_cast<const std::size_t*>(offsets_region_.data());
  }

  [[nodiscard]] auto chars() const noexcept -> const char* {
    return reinterpret_cast<const char*>(chars_region_.data());
  }

  [[nodiscard]] auto at(std::size_t row) const noexcept -> std::string_view {
    const auto begin = offsets()[row];
    const auto end = offsets()[row + 1U];
    return {chars() + begin, end - begin};
  }

 private:
  detail::mapped_region offsets_region_{};
  detail::mapped_region chars_region_{};
  std::size_t row_count_{0U};
  std::size_t chars_size_{0U};

  friend class mmap_storage_policy;
};

struct heap_storage_policy : storage_policy_base<heap_storage_policy> {
  template <typename T>
  using column = heap_column<T>;

  template <typename T>
  auto make_empty_impl(std::size_t) const -> std::shared_ptr<column<T>> {
    return std::make_shared<column<T>>();
  }

  template <typename T>
  auto clone_append_impl(std::shared_ptr<const column<T>> current, batch_column_view<T> batch,
                         std::size_t) const -> std::shared_ptr<column<T>> {
    return column<T>::append(current.get(), batch);
  }
};

class mmap_storage_policy : public storage_policy_base<mmap_storage_policy> {
 public:
  template <typename T>
  using column = mmap_column<T>;

  explicit mmap_storage_policy(std::filesystem::path root_directory = std::filesystem::temp_directory_path() /
                                                                  "dpe-mmap")
      : root_directory_{std::move(root_directory)} {
    std::filesystem::create_directories(root_directory_);
  }

  mmap_storage_policy(const mmap_storage_policy& other)
      : root_directory_{other.root_directory_},
        sequence_{other.sequence_.load(std::memory_order_relaxed)} {
    std::filesystem::create_directories(root_directory_);
  }

  auto operator=(const mmap_storage_policy& other) -> mmap_storage_policy& {
    if (this == &other) {
      return *this;
    }

    root_directory_ = other.root_directory_;
    sequence_.store(other.sequence_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    std::filesystem::create_directories(root_directory_);
    return *this;
  }

  mmap_storage_policy(mmap_storage_policy&& other) noexcept
      : root_directory_{std::move(other.root_directory_)},
        sequence_{other.sequence_.load(std::memory_order_relaxed)} {}

  auto operator=(mmap_storage_policy&& other) noexcept -> mmap_storage_policy& {
    if (this == &other) {
      return *this;
    }

    root_directory_ = std::move(other.root_directory_);
    sequence_.store(other.sequence_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    return *this;
  }

  template <typename T>
  auto make_empty_impl(std::size_t) const -> std::shared_ptr<column<T>> {
    return std::make_shared<column<T>>();
  }

  template <typename T>
    requires(!std::same_as<T, std::string>)
  auto clone_append_impl(std::shared_ptr<const column<T>> current, batch_column_view<T> batch,
                         std::size_t column_index) const -> std::shared_ptr<column<T>> {
    const auto existing_size = current == nullptr ? 0U : current->size();
    const auto new_size = existing_size + batch.size();
    if (new_size == 0U) {
      return std::make_shared<column<T>>();
    }

    const auto path = next_path(column_index, ".bin");
    detail::mapped_region region{path, new_size * sizeof(T), detail::mapping_access::read_write};
    auto* destination = reinterpret_cast<T*>(region.data());

    if (current != nullptr && existing_size != 0U) {
      std::copy_n(current->data(), existing_size, destination);
    }
    std::copy(batch.values.begin(), batch.values.end(), destination + existing_size);
    region.flush();

    return std::make_shared<column<T>>(std::move(region), new_size);
  }

  auto clone_append_impl(std::shared_ptr<const column<std::string>> current,
                         batch_column_view<std::string> batch, std::size_t column_index) const
      -> std::shared_ptr<column<std::string>> {
    const auto existing_rows = current == nullptr ? 0U : current->size();
    const auto existing_chars = current == nullptr ? 0U : current->chars_size();
    const auto new_rows = existing_rows + batch.size();
    const auto additional_chars =
        std::transform_reduce(batch.values.begin(), batch.values.end(), std::size_t{0U},
                              std::plus<>{},
                              [](std::string_view value) { return value.size(); });
    const auto new_chars = existing_chars + additional_chars;

    if (new_rows == 0U) {
      return std::make_shared<column<std::string>>();
    }

    const auto offsets_path = next_path(column_index, ".offsets");
    const auto chars_path = next_path(column_index, ".chars");

    detail::mapped_region offsets_region{
        offsets_path, (new_rows + 1U) * sizeof(std::size_t), detail::mapping_access::read_write};
    detail::mapped_region chars_region{chars_path, std::max<std::size_t>(1U, new_chars),
                                       detail::mapping_access::read_write};

    auto* offsets = reinterpret_cast<std::size_t*>(offsets_region.data());
    auto* chars = reinterpret_cast<char*>(chars_region.data());

    if (current != nullptr && existing_rows != 0U) {
      std::copy_n(current->offsets(), existing_rows + 1U, offsets);
      std::copy_n(current->chars(), existing_chars, chars);
    } else {
      offsets[0] = 0U;
    }

    auto cursor = existing_chars;
    auto offset_index = existing_rows;
    for (const auto value : batch.values) {
      std::memcpy(chars + cursor, value.data(), value.size());
      cursor += value.size();
      ++offset_index;
      offsets[offset_index] = cursor;
    }

    offsets_region.flush();
    chars_region.flush();

    return std::make_shared<column<std::string>>(std::move(offsets_region), std::move(chars_region),
                                                 new_rows, new_chars);
  }

 private:
  [[nodiscard]] auto next_path(std::size_t column_index, std::string_view suffix) const
      -> std::filesystem::path {
    const auto id = sequence_.fetch_add(1U, std::memory_order_relaxed);
    return root_directory_ /
           ("column-" + std::to_string(column_index) + "-" + std::to_string(id) +
            std::string{suffix});
  }

  std::filesystem::path root_directory_;
  mutable std::atomic<std::uint64_t> sequence_{0U};
};

}  // namespace dpe
