# Data Processing Engine

`data-processing-engine` is a modern C++20 columnar analytics engine built around compile-time schemas, expression templates, immutable snapshots, and policy-based execution.

This snapshot introduces the full in-memory engine and end-to-end demo pipeline. Validation and benchmarking land in the next step.

## Highlights

- Compile-time schemas with `schema<field<"...", T>, ...>`
- Lazy expression trees built from overloaded operators
- Columnar storage with heap and memory-mapped policies
- Snapshot-based table API with lock-free reads
- Filter, select, group-by, and aggregate query pipeline
- Sequenced and parallel execution policies

## Example

```cpp
using sales_schema = dpe::schema<
    dpe::field<"order_id", std::int64_t>,
    dpe::field<"category", std::string>,
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

dpe::table<sales_schema, dpe::heap_storage_policy, dpe::parallel_execution> sales;

const auto revenue = dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">());

auto summary =
    sales
        .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
        .select(dpe::as<"category">(dpe::col<"category">()), revenue)
        .groupBy(dpe::name_v<"category">)
        .aggregate(dpe::count<"orders">(),
                   dpe::sum<"revenue", "gross_revenue">(),
                   dpe::avg<"revenue", "avg_revenue">());
```

## Build

Direct compile:

```bash
c++ -std=c++20 -Iinclude main.cpp src/thread_pool.cpp src/mmap_region.cpp -o dpe-demo
./dpe-demo
```

Or use CMake:

```bash
cmake --preset debug
cmake --build --preset build-debug
```

## Layout

- `include/dpe`: public headers
- `include/dpe/detail`: runtime internals
- `src`: thread-pool and memory-mapping implementations
- `main.cpp`: end-to-end usage demo
