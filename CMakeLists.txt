cmake_minimum_required(VERSION 3.27)

project(
  dpe
  VERSION 0.1.0
  DESCRIPTION "Compile-time type-safe columnar data processing engine in modern C++20"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(DPE_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(DPE_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

include(CheckCXXSourceCompiles)

set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++20")

check_cxx_source_compiles(
  "
    #include <memory>
    int main() {
      std::shared_ptr<int> ptr;
      std::atomic_store(&ptr, std::make_shared<int>(42));
      auto loaded = std::atomic_load(&ptr);
      return *loaded - 42;
    }
  "
  DPE_HAS_ATOMIC_SHARED_PTR_FREE_FUNCTIONS)

check_cxx_source_compiles(
  "
    #if __has_include(<execution>)
    #include <execution>
    #endif
    int main() {
#if defined(__cpp_lib_execution) && (__cpp_lib_execution >= 201902L)
      auto policy = std::execution::par_unseq;
      (void)policy;
#endif
      return 0;
    }
  "
  DPE_HAS_STD_EXECUTION)

if(NOT DPE_HAS_ATOMIC_SHARED_PTR_FREE_FUNCTIONS)
  message(
    FATAL_ERROR
      "The selected standard library does not support atomic shared_ptr load/store free functions. "
      "Use a newer C++20 toolchain.")
endif()

if(DPE_HAS_STD_EXECUTION)
  message(STATUS "std::execution detected: enabling execution-policy fast paths.")
else()
  message(WARNING
          "std::execution fast paths are unavailable on this toolchain. "
          "The engine will still build and use the internal thread pool fallback.")
endif()

function(dpe_apply_warnings target_name)
  if(MSVC)
    target_compile_options(${target_name} PRIVATE /W4 /permissive-)
  else()
    target_compile_options(
      ${target_name}
      PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wconversion
        -Wsign-conversion
        -Wold-style-cast)
  endif()
endfunction()

function(dpe_enable_sanitizers target_name)
  if(MSVC)
    return()
  endif()

  if(DPE_ENABLE_ASAN)
    target_compile_options(${target_name} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(${target_name} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
  endif()

  if(DPE_ENABLE_UBSAN)
    target_compile_options(${target_name} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(${target_name} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer)
  endif()
endfunction()

add_library(
  dpe_core STATIC
  src/mmap_region.cpp
  src/thread_pool.cpp)

target_include_directories(dpe_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_features(dpe_core PUBLIC cxx_std_20)

dpe_apply_warnings(dpe_core)
dpe_enable_sanitizers(dpe_core)

add_executable(dpe_demo main.cpp)
target_link_libraries(dpe_demo PRIVATE dpe_core)
dpe_apply_warnings(dpe_demo)
dpe_enable_sanitizers(dpe_demo)
