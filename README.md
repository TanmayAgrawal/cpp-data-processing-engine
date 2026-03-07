# Data Processing Engine

`data-processing-engine` is a modern C++20 columnar analytics engine built to showcase deep systems-level C++: compile-time schemas, expression templates, policy-based design with CRTP, immutable snapshots for lock-free reads, and cache-aware grouped aggregation.

The code is intentionally header-heavy for the type system and query DSL, with only OS/runtime concerns implemented in `.cpp` files.

## Highlights

- Compile-time schema definition with `schema<field<"price", double>, ...>`
- Type-safe column access and query construction with lazy expression templates
- Immutable column snapshots published via `std::atomic<std::shared_ptr<...>>`
- Pluggable storage policies: heap and file-backed memory-mapped
- Pluggable execution policies: sequenced and parallel
- Filter, select, group-by, and aggregate pipeline with compile-time validation
- Google Benchmark harness with a 10M-row fixed-seed dataset and row-store baseline

## Architecture

### 1. Compile-time schema and names

The schema layer lives under [`include/dpe`](./include/dpe):

- `fixed_string.hpp`: class-type NTTP wrapper used for compile-time column names
- `schema.hpp`: `field`, `schema`, `column_type_t`, compile-time lookup, uniqueness checks
- `concepts.hpp`: engine-supported logical types and normalization rules

Key idea: the schema is the source of truth for both storage layout and the query DSL. Invalid column names and unsupported logical types fail at compile time.

### 2. Storage model

Storage is column-oriented and type-specialized:

- Numeric columns store aligned, contiguous values
- String columns store `offsets + char arena`
- Table writes are append-only and publish a fresh immutable snapshot

Readers never lock. They atomically load the current `shared_ptr<const snapshot>` and evaluate against that frozen view. Writers acquire per-column `shared_mutex` instances in schema order, clone/append every column, and atomically publish the new snapshot.

### 3. Expression templates

Expressions are lazy trees, not eager temporaries:

```cpp
auto revenue = dpe::col<"price">() * dpe::col<"quantity">();
auto predicate = revenue > 1000.0;
```

`column_ref`, `literal`, `unary_expr`, and `binary_expr` carry type information all the way through the pipeline. Expressions are only materialized inside terminal operations like `evaluate()` or `aggregate()`.

The non-obvious piece is the relation binding model:

- every relation carries a compile-time "virtual schema"
- `select()` binds aliases to root expressions instead of materializing immediately
- later operations like `filter()` and `groupBy()` are rebound back to the root snapshot

That means:

- `select(as<"revenue">(price * qty))` stays lazy
- `groupBy<"revenue">()` after `select()` remains compile-time safe
- we never need runtime column-name dispatch

Because standard C++20 cannot express a true `col("price")` function with a string literal and still keep the name in the type system, the DSL exposes three zero-overhead entry points:

- `col<"price">()` as the canonical API
- `col(name_v<"price">)` when you want to avoid dependent-template syntax
- `DPE_COL("price")` as a thin macro for demo-friendly call sites

The same `name_v<...>` tag style also works with `as(...)`, `groupBy(...)`, `value(...)`, and the aggregate builders.

### 4. Query pipeline

The public pipeline is:

```cpp
table
  .filter(...)
  .select(...)
  .groupBy<...>()
  .aggregate(...);
```

Supported terminals:

- `evaluate()`
- `aggregate(...)`

Supported aggregates:

- `count<Alias>()`
- `sum<Column, Alias>()`
- `min<Column, Alias>()`
- `max<Column, Alias>()`
- `avg<Column, Alias>()`

Rules are enforced at compile time:

- unknown columns fail to instantiate
- duplicate schema fields fail
- invalid arithmetic on strings fails
- numeric aggregates on non-numeric columns fail
- aggregating a column that disappeared from the current pipeline fails

### 5. GroupBy implementation

Grouped aggregation uses an internal open-addressing hash table in [`include/dpe/detail/group_map.hpp`](./include/dpe/detail/group_map.hpp):

- linear probing
- power-of-two capacity
- tuple hashing for compile-time key tuples
- per-thread local maps under `parallel_execution`
- merge step after local aggregation

String keys are stored as `std::string_view` into the immutable snapshot during aggregation, so the hot group-by path does not allocate per row.

### 6. Policy-based design

#### Storage policies

- `heap_storage_policy`
- `mmap_storage_policy`

Both satisfy the same compile-time interface:

- `make_empty<T>(column_index)`
- `clone_append<T>(current, batch, column_index)`

#### Execution policies

- `sequenced_execution`
- `parallel_execution`

Both expose:

- `for_chunks(total_rows, grain_rows, fn)`
- `suggested_grain_rows(...)`
- `vectorized_for(...)`

Dispatch is resolved at compile time through the policy type. The parallel policy combines:

- a shared thread pool for chunk-level parallel work
- `std::execution::par_unseq` / `unseq` for vectorizable inner loops

## Build

### Toolchain requirements

This project prefers a recent Clang/LLVM or GCC toolchain, but it now degrades gracefully when the standard library does not expose `std::execution`. You need:

- CMake 3.27+
- support for atomic `shared_ptr` load/store free functions
- Google Benchmark installed for `DPE_BUILD_BENCHMARKS=ON`

Fast path behavior:

- with `std::execution`, inner loops use `unseq` / `par_unseq`
- without it, the engine still builds and keeps chunk-level parallelism through the internal thread pool

### Presets

Available presets:

- `debug`
- `asan`
- `ubsan`
- `release-bench`

Typical workflow:

```bash
cmake --preset debug
cmake --build --preset build-debug
ctest --preset test-debug
```

The debug, ASan, and UBSan presets keep benchmarks off by default so a normal development configure does not depend on Google Benchmark being installed. The `release-bench` preset turns benchmark builds on explicitly.

Benchmark build:

```bash
cmake --preset release-bench
cmake --build --preset build-release-bench
./build/release-bench/dpe_benchmarks \
  --benchmark_repetitions=5 \
  --benchmark_report_aggregates_only=true \
  --benchmark_min_time=0.5s \
  --benchmark_counters_tabular=true
```

## Example

The demo in [`main.cpp`](./main.cpp) builds an end-to-end pipeline:

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
        .aggregate(
            dpe::count<"orders">(),
            dpe::sum<"revenue", "gross_revenue">(),
            dpe::avg<"revenue", "avg_revenue">());
```

## Testing

The test suite contains:

- runtime correctness tests in [`tests/runtime_tests.cpp`](./tests/runtime_tests.cpp)
- compile-fail tests driven by CTest + direct compiler invocations

Compile-fail coverage includes:

- duplicate schema fields
- duplicate projection aliases
- duplicate aggregate aliases or key/alias collisions
- unknown columns
- invalid arithmetic expressions
- numeric aggregation on strings
- aggregating columns that are no longer present after a projection

Runtime coverage includes:

- batch row-count validation
- string predicates and string projection paths
- relation snapshot capture across later appends
- heap vs mmap parity
- sequenced vs parallel result parity

## Benchmark Harness

The benchmark suite lives in [`benchmarks/bench_engine.cpp`](./benchmarks/bench_engine.cpp) and compares:

- columnar filter vs row-store filter
- columnar projection vs row-store projection
- columnar grouped aggregation vs row-store grouped aggregation

Methodology:

- fixed seed: `20260402`
- dataset size: `10,000,000` rows
- mixed numeric + string columns
- output metric: `rows/sec`

## Benchmark Results

Measured on April 2, 2026 on an Apple M3 Pro (12 CPU cores, 18 GB RAM) using Apple Clang 16.0.0 and Google Benchmark 1.9.5. All benchmark registrations use `UseRealTime()`, and the table below reports the median rows/sec across 5 repetitions with a `0.5s` minimum run time per benchmark. Filter, projection, and aggregation were run in pairs to reduce cross-benchmark thermal interference on laptop-class hardware.

| Benchmark | Rows/sec | Notes |
| --- | ---: | --- |
| Columnar Filter | 76.2M | high-value order filter + lazy revenue expression |
| Row Filter | 161.1M | naive row loop baseline |
| Columnar Projection | 554.9M | materialized `order_id + revenue` |
| Row Projection | 280.1M | naive row loop baseline |
| Columnar Aggregation | 52.7M | group by string `category` with `count/sum/avg` |
| Row Aggregation | 63.6M | `unordered_map` baseline |

The engine clears the 10M+ rows/sec target across all measured workloads on this machine. Projection is the strongest path today at roughly 2x the row-store baseline, while filter and string-key aggregation still leave optimization headroom versus the naive row implementation.

## Repository Layout

- [`include/dpe`](./include/dpe): public library headers
- [`src`](./src): mmap + thread-pool runtime pieces
- [`tests`](./tests): runtime and compile-fail tests
- [`benchmarks`](./benchmarks): Google Benchmark comparisons
- [`main.cpp`](./main.cpp): end-to-end usage example
