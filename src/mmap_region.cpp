#include "dpe/detail/mmap_region.hpp"

#include <stdexcept>
#include <string>
#include <system_error>

#ifdef _WIN32
#include <Windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace dpe::detail {

namespace {

[[noreturn]] void throw_system(std::string message) {
#ifdef _WIN32
  throw std::system_error{
      static_cast<int>(::GetLastError()), std::system_category(), std::move(message)};
#else
  throw std::system_error{errno, std::generic_category(), std::move(message)};
#endif
}

}  // namespace

mapped_region::mapped_region(const std::filesystem::path& path, std::size_t bytes,
                             mapping_access access)
    : size_{bytes} {
  if (bytes == 0U) {
    return;
  }

  if (!path.parent_path().empty()) {
    std::filesystem::create_directories(path.parent_path());
  }

#ifdef _WIN32
  const auto wide_path = path.wstring();
  const auto desired_access = access == mapping_access::read_write ? GENERIC_READ | GENERIC_WRITE
                                                                   : GENERIC_READ;
  const auto creation = access == mapping_access::read_write ? CREATE_ALWAYS : OPEN_EXISTING;

  file_handle_ = ::CreateFileW(wide_path.c_str(), desired_access, FILE_SHARE_READ, nullptr,
                               creation, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (file_handle_ == INVALID_HANDLE_VALUE) {
    file_handle_ = nullptr;
    throw_system("CreateFileW failed for mapped_region");
  }

  LARGE_INTEGER size_value{};
  size_value.QuadPart = static_cast<LONGLONG>(bytes);

  if (access == mapping_access::read_write &&
      (!::SetFilePointerEx(static_cast<HANDLE>(file_handle_), size_value, nullptr, FILE_BEGIN) ||
       !::SetEndOfFile(static_cast<HANDLE>(file_handle_)))) {
    close();
    throw_system("SetEndOfFile failed for mapped_region");
  }

  const auto protect = access == mapping_access::read_write ? PAGE_READWRITE : PAGE_READONLY;
  mapping_handle_ =
      ::CreateFileMappingW(static_cast<HANDLE>(file_handle_), nullptr, protect,
                           size_value.HighPart, size_value.LowPart, nullptr);
  if (mapping_handle_ == nullptr) {
    close();
    throw_system("CreateFileMappingW failed for mapped_region");
  }

  const auto map_access = access == mapping_access::read_write ? FILE_MAP_ALL_ACCESS : FILE_MAP_READ;
  data_ = static_cast<std::byte*>(
      ::MapViewOfFile(static_cast<HANDLE>(mapping_handle_), map_access, 0, 0, bytes));
  if (data_ == nullptr) {
    close();
    throw_system("MapViewOfFile failed for mapped_region");
  }
#else
  const auto flags = access == mapping_access::read_write ? (O_RDWR | O_CREAT | O_TRUNC) : O_RDONLY;
  fd_ = ::open(path.c_str(), flags, 0600);
  if (fd_ == -1) {
    throw_system("open failed for mapped_region");
  }

  if (access == mapping_access::read_write &&
      ::ftruncate(fd_, static_cast<off_t>(bytes)) != 0) {
    close();
    throw_system("ftruncate failed for mapped_region");
  }

  const auto protect = access == mapping_access::read_write ? (PROT_READ | PROT_WRITE) : PROT_READ;
  void* mapping = ::mmap(nullptr, bytes, protect, MAP_SHARED, fd_, 0);
  if (mapping == MAP_FAILED) {
    data_ = nullptr;
    close();
    throw_system("mmap failed for mapped_region");
  }

  data_ = static_cast<std::byte*>(mapping);
#endif
}

mapped_region::mapped_region(mapped_region&& other) noexcept {
  *this = std::move(other);
}

auto mapped_region::operator=(mapped_region&& other) noexcept -> mapped_region& {
  if (this == &other) {
    return *this;
  }

  close();

#ifdef _WIN32
  file_handle_ = other.file_handle_;
  mapping_handle_ = other.mapping_handle_;
  other.file_handle_ = nullptr;
  other.mapping_handle_ = nullptr;
#else
  fd_ = other.fd_;
  other.fd_ = -1;
#endif

  data_ = other.data_;
  size_ = other.size_;

  other.data_ = nullptr;
  other.size_ = 0U;
  return *this;
}

mapped_region::~mapped_region() {
  close();
}

auto mapped_region::data() noexcept -> std::byte* {
  return data_;
}

auto mapped_region::data() const noexcept -> const std::byte* {
  return data_;
}

auto mapped_region::size() const noexcept -> std::size_t {
  return size_;
}

void mapped_region::flush() {
  if (data_ == nullptr || size_ == 0U) {
    return;
  }

#ifdef _WIN32
  if (!::FlushViewOfFile(data_, size_)) {
    throw_system("FlushViewOfFile failed for mapped_region");
  }
#else
  if (::msync(data_, size_, MS_SYNC) != 0) {
    throw_system("msync failed for mapped_region");
  }
#endif
}

void mapped_region::close() noexcept {
#ifdef _WIN32
  if (data_ != nullptr) {
    ::UnmapViewOfFile(data_);
    data_ = nullptr;
  }

  if (mapping_handle_ != nullptr) {
    ::CloseHandle(static_cast<HANDLE>(mapping_handle_));
    mapping_handle_ = nullptr;
  }

  if (file_handle_ != nullptr) {
    ::CloseHandle(static_cast<HANDLE>(file_handle_));
    file_handle_ = nullptr;
  }
#else
  if (data_ != nullptr && size_ != 0U) {
    ::munmap(data_, size_);
    data_ = nullptr;
  }

  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
#endif

  size_ = 0U;
}

}  // namespace dpe::detail
