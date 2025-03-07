#pragma once

#include <bit>
#include <algorithm>
#include <cstddef>
#include <functional>
#include <tuple>
#include <utility>
#include <vector>

namespace dpe::detail {

template <typename T>
inline void hash_combine(std::size_t& seed, const T& value) {
  seed ^= std::hash<T>{}(value) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
}

template <typename Tuple, std::size_t... I>
inline auto tuple_hash_impl(const Tuple& tuple, std::index_sequence<I...>) -> std::size_t {
  std::size_t seed = 0U;
  (hash_combine(seed, std::get<I>(tuple)), ...);
  return seed;
}

template <typename Tuple>
struct tuple_hash;

template <typename... Ts>
struct tuple_hash<std::tuple<Ts...>> {
  [[nodiscard]] auto operator()(const std::tuple<Ts...>& tuple) const -> std::size_t {
    return tuple_hash_impl(tuple, std::index_sequence_for<Ts...>{});
  }
};

template <typename Key, typename State, typename Hash = tuple_hash<Key>,
          typename Eq = std::equal_to<Key>>
class open_addressing_table {
 public:
  explicit open_addressing_table(std::size_t expected_size = 16U) {
    const auto capacity = std::max<std::size_t>(8U, std::bit_ceil(expected_size * 2U));
    slots_.resize(capacity);
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return size_;
  }

  auto find_or_emplace(const Key& key) -> State& {
    if ((size_ + 1U) * 10U >= slots_.size() * 7U) {
      rehash(slots_.size() * 2U);
    }

    auto index = hash_(key) & (slots_.size() - 1U);
    while (slots_[index].occupied && !eq_(slots_[index].key, key)) {
      index = (index + 1U) & (slots_.size() - 1U);
    }

    if (!slots_[index].occupied) {
      slots_[index].occupied = true;
      slots_[index].key = key;
      slots_[index].state = State{};
      ++size_;
    }

    return slots_[index].state;
  }

  template <typename Fn>
  void for_each_occupied(Fn&& fn) const {
    for (const auto& slot : slots_) {
      if (slot.occupied) {
        fn(slot.key, slot.state);
      }
    }
  }

 private:
  struct slot {
    bool occupied{false};
    Key key{};
    State state{};
  };

  void rehash(std::size_t new_capacity) {
    std::vector<slot> old_slots = std::move(slots_);
    slots_.clear();
    slots_.resize(std::bit_ceil(std::max<std::size_t>(8U, new_capacity)));
    size_ = 0U;

    for (const auto& slot : old_slots) {
      if (!slot.occupied) {
        continue;
      }

      auto& destination = find_or_emplace(slot.key);
      destination = slot.state;
    }
  }

  std::vector<slot> slots_{};
  std::size_t size_{0U};
  Hash hash_{};
  Eq eq_{};
};

}  // namespace dpe::detail
