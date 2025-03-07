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

option(DPE_BUILD_TESTS "Build runtime and compile-fail tests" ON)
option(DPE_BUILD_BENCHMARKS "Build Google Benchmark targets" OFF)
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

if(DPE_BUILD_TESTS)
  enable_testing()

  add_executable(dpe_runtime_tests tests/runtime_tests.cpp)
  target_link_libraries(dpe_runtime_tests PRIVATE dpe_core)
  dpe_apply_warnings(dpe_runtime_tests)
  dpe_enable_sanitizers(dpe_runtime_tests)

  add_test(NAME dpe_runtime_tests COMMAND dpe_runtime_tests)

  function(dpe_add_compile_fail_test test_name source_file)
    add_test(
      NAME ${test_name}
      COMMAND
        ${CMAKE_COMMAND}
        -DCASE_SOURCE=${source_file}
        -DBINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}/compile-fail/${test_name}
        -DCXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCXX_STANDARD=${CMAKE_CXX_STANDARD}
        -DDPE_INCLUDE_DIR=${PROJECT_SOURCE_DIR}/include
        -P ${PROJECT_SOURCE_DIR}/tests/compile_fail_case.cmake)
  endfunction()

  dpe_add_compile_fail_test(
    compile_fail_duplicate_schema
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/duplicate_schema.cpp)
  dpe_add_compile_fail_test(
    compile_fail_unknown_column
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/unknown_column.cpp)
  dpe_add_compile_fail_test(
    compile_fail_invalid_expression
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/invalid_expression.cpp)
  dpe_add_compile_fail_test(
    compile_fail_string_sum
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/string_sum.cpp)
  dpe_add_compile_fail_test(
    compile_fail_missing_pipeline_column
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/missing_pipeline_column.cpp)
  dpe_add_compile_fail_test(
    compile_fail_duplicate_projection_alias
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/duplicate_projection_alias.cpp)
  dpe_add_compile_fail_test(
    compile_fail_duplicate_aggregate_alias
    ${PROJECT_SOURCE_DIR}/tests/compile_fail/duplicate_aggregate_alias.cpp)
endif()

if(DPE_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG QUIET)
  if(NOT benchmark_FOUND)
    message(
      FATAL_ERROR
        "Google Benchmark was not found. Install it and expose benchmark::benchmark, or configure with -DDPE_BUILD_BENCHMARKS=OFF.")
  endif()

  add_executable(dpe_benchmarks benchmarks/bench_engine.cpp)
  target_link_libraries(dpe_benchmarks PRIVATE dpe_core benchmark::benchmark
                                               benchmark::benchmark_main)
  dpe_apply_warnings(dpe_benchmarks)
  dpe_enable_sanitizers(dpe_benchmarks)
endif()
