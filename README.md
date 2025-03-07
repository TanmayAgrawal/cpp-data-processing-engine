# Data Processing Engine

This repository starts with the compile-time foundation for a columnar analytics engine in modern C++20.

## What Is Included

- `fixed_string` for compile-time column names
- `field` and `schema` for type-safe compile-time schemas
- `batch` helpers for span-based column views
- lazy expression templates built from `col<"...">()` and overloaded operators

The first snapshot focuses on the type system and expression DSL before storage, execution, and query runtime layers are introduced.

## Example

```cpp
using sales_schema = dpe::schema<
    dpe::field<"order_id", std::int64_t>,
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

auto revenue = dpe::col<"price">() * dpe::col<"quantity">();
auto high_value = revenue > 1000.0;
```

## Build

Direct compile:

```bash
c++ -std=c++20 -Iinclude main.cpp -o dpe-demo
./dpe-demo
```

Or use CMake presets:

```bash
cmake --preset debug
cmake --build --preset build-debug
```
