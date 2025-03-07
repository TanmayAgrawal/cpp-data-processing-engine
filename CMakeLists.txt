cmake_minimum_required(VERSION 3.27)

project(
  dpe
  VERSION 0.1.0
  DESCRIPTION "Compile-time schema and expression foundation for a C++20 columnar engine"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(dpe_foundation INTERFACE)
target_include_directories(dpe_foundation INTERFACE ${PROJECT_SOURCE_DIR}/include)
target_compile_features(dpe_foundation INTERFACE cxx_std_20)

add_executable(dpe_demo main.cpp)
target_link_libraries(dpe_demo PRIVATE dpe_foundation)

if(MSVC)
  target_compile_options(dpe_demo PRIVATE /W4 /permissive-)
else()
  target_compile_options(
    dpe_demo
    PRIVATE
      -Wall
      -Wextra
      -Wpedantic
      -Wshadow
      -Wconversion
      -Wsign-conversion)
endif()
