#pragma once

#include <cstddef>
#include <filesystem>

namespace dpe::detail {

enum class mapping_access {
  read_only,
  read_write,
};

class mapped_region {
 public:
  mapped_region() = default;
  mapped_region(const std::filesystem::path& path, std::size_t bytes, mapping_access access);
  mapped_region(mapped_region&& other) noexcept;
  auto operator=(mapped_region&& other) noexcept -> mapped_region&;
  mapped_region(const mapped_region&) = delete;
  auto operator=(const mapped_region&) -> mapped_region& = delete;
  ~mapped_region();

  [[nodiscard]] auto data() noexcept -> std::byte*;
  [[nodiscard]] auto data() const noexcept -> const std::byte*;
  [[nodiscard]] auto size() const noexcept -> std::size_t;
  void flush();

 private:
  void close() noexcept;

#ifdef _WIN32
  void* file_handle_{nullptr};
  void* mapping_handle_{nullptr};
#else
  int fd_{-1};
#endif
  std::byte* data_{nullptr};
  std::size_t size_{0U};
};

}  // namespace dpe::detail
