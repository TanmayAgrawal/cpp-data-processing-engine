#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <memory>
#include <random>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dpe/dpe.hpp"

namespace {

using bench_schema = dpe::schema<
    dpe::field<"order_id", std::int64_t>,
    dpe::field<"category", std::string>,
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

struct row_record {
  std::int64_t order_id{};
  std::string_view category{};
  double price{};
  std::int32_t quantity{};
};

struct benchmark_dataset {
  std::vector<std::int64_t> order_ids;
  std::vector<std::string_view> categories;
  std::vector<double> prices;
  std::vector<std::int32_t> quantities;
  std::vector<row_record> rows;

  static auto make(std::size_t row_count) -> benchmark_dataset {
    benchmark_dataset dataset;
    dataset.order_ids.reserve(row_count);
    dataset.categories.reserve(row_count);
    dataset.prices.reserve(row_count);
    dataset.quantities.reserve(row_count);
    dataset.rows.reserve(row_count);

    constexpr std::array<std::string_view, 8> category_pool{
        "books", "games", "hardware", "software",
        "music", "office", "garden", "kitchen"};

    std::mt19937_64 rng{20260402ULL};
    std::uniform_real_distribution<double> price_dist{10.0, 1500.0};
    std::uniform_int_distribution<std::int32_t> quantity_dist{1, 8};
    std::uniform_int_distribution<std::size_t> category_dist{0U, category_pool.size() - 1U};

    for (std::size_t row = 0U; row < row_count; ++row) {
      const auto category = category_pool[category_dist(rng)];
      const auto price = price_dist(rng);
      const auto quantity = quantity_dist(rng);

      dataset.order_ids.push_back(static_cast<std::int64_t>(row));
      dataset.categories.push_back(category);
      dataset.prices.push_back(price);
      dataset.quantities.push_back(quantity);
      dataset.rows.push_back(
          row_record{static_cast<std::int64_t>(row), category, price, quantity});
    }

    return dataset;
  }
};

constexpr auto kBenchmarkRows = std::size_t{10'000'000U};

auto dataset() -> const benchmark_dataset& {
  static const auto value = benchmark_dataset::make(kBenchmarkRows);
  return value;
}

auto engine() -> dpe::table<bench_schema, dpe::heap_storage_policy, dpe::parallel_execution>& {
  static auto table = [] {
    auto value = std::make_unique<
        dpe::table<bench_schema, dpe::heap_storage_policy, dpe::parallel_execution>>(
        dpe::heap_storage_policy{},
        dpe::parallel_execution{std::max<std::size_t>(1U, std::thread::hardware_concurrency())});

    const auto& data = dataset();
    value->appendBatch(dpe::make_batch<bench_schema>(
        std::span{data.order_ids}, std::span{data.categories}, std::span{data.prices},
        std::span{data.quantities}));
    return value;
  }();

  return *table;
}

static void BM_ColumnarFilter(benchmark::State& state) {
  auto& table = engine();
  const auto rows = dataset().order_ids.size();

  for (auto _ : state) {
    auto result = table
                      .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
                      .select(dpe::as<"category">(dpe::col<"category">()),
                              dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">()))
                      .evaluate();
    benchmark::DoNotOptimize(result.row_count());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
  state.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(state.iterations() * rows),
                         benchmark::Counter::kIsRate);
}

static void BM_RowFilter(benchmark::State& state) {
  const auto& rows = dataset().rows;

  for (auto _ : state) {
    std::vector<std::string_view> categories;
    std::vector<double> revenue;
    categories.reserve(rows.size() / 2U);
    revenue.reserve(rows.size() / 2U);

    for (const auto& row : rows) {
      const auto row_revenue = row.price * static_cast<double>(row.quantity);
      if (row_revenue > 1000.0) {
        categories.push_back(row.category);
        revenue.push_back(row_revenue);
      }
    }

    benchmark::DoNotOptimize(categories.size());
    benchmark::DoNotOptimize(revenue.data());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows.size()));
  state.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(state.iterations() * rows.size()),
                         benchmark::Counter::kIsRate);
}

static void BM_ColumnarProjection(benchmark::State& state) {
  auto& table = engine();
  const auto rows = dataset().order_ids.size();

  for (auto _ : state) {
    auto result = table
                      .select(dpe::as<"order_id">(dpe::col<"order_id">()),
                              dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">()))
                      .evaluate();
    benchmark::DoNotOptimize(result.row_count());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
  state.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(state.iterations() * rows),
                         benchmark::Counter::kIsRate);
}

static void BM_RowProjection(benchmark::State& state) {
  const auto& rows = dataset().rows;

  for (auto _ : state) {
    std::vector<std::int64_t> order_ids;
    std::vector<double> revenue;
    order_ids.reserve(rows.size());
    revenue.reserve(rows.size());

    for (const auto& row : rows) {
      order_ids.push_back(row.order_id);
      revenue.push_back(row.price * static_cast<double>(row.quantity));
    }

    benchmark::DoNotOptimize(order_ids.data());
    benchmark::DoNotOptimize(revenue.data());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows.size()));
  state.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(state.iterations() * rows.size()),
                         benchmark::Counter::kIsRate);
}

static void BM_ColumnarAggregation(benchmark::State& state) {
  auto& table = engine();
  const auto rows = dataset().order_ids.size();

  for (auto _ : state) {
    auto result =
        table
            .select(dpe::as<"category">(dpe::col<"category">()),
                    dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">()))
            .groupBy(dpe::name_v<"category">)
            .aggregate(dpe::count<"orders">(), dpe::sum<"revenue", "gross_revenue">(),
                       dpe::avg<"revenue", "avg_revenue">());
    benchmark::DoNotOptimize(result.row_count());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
  state.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(state.iterations() * rows),
                         benchmark::Counter::kIsRate);
}

static void BM_RowAggregation(benchmark::State& state) {
  const auto& rows = dataset().rows;

  for (auto _ : state) {
    struct stats {
      std::int64_t count{0};
      double sum{0.0};
    };

    std::unordered_map<std::string_view, stats> aggregates;
    aggregates.reserve(16U);

    for (const auto& row : rows) {
      auto& state_ref = aggregates[row.category];
      ++state_ref.count;
      state_ref.sum += row.price * static_cast<double>(row.quantity);
    }

    benchmark::DoNotOptimize(aggregates.size());
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows.size()));
  state.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(state.iterations() * rows.size()),
                         benchmark::Counter::kIsRate);
}

BENCHMARK(BM_ColumnarFilter);
BENCHMARK(BM_RowFilter);
BENCHMARK(BM_ColumnarProjection);
BENCHMARK(BM_RowProjection);
BENCHMARK(BM_ColumnarAggregation);
BENCHMARK(BM_RowAggregation);

}  // namespace
