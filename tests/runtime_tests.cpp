#include <cmath>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "dpe/dpe.hpp"

namespace {

using sales_schema = dpe::schema<
    dpe::field<"order_id", std::int64_t>,
    dpe::field<"category", std::string>,
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

struct test_failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};

void expect(bool condition, std::string_view message) {
  if (!condition) {
    throw test_failure{std::string{message}};
  }
}

template <typename Table>
void load_sales_data(Table& table) {
  std::vector<std::int64_t> order_ids{1001, 1002, 1003, 1004, 1005, 1006};
  std::vector<std::string_view> categories{
      "books", "hardware", "books", "software", "hardware", "software"};
  std::vector<double> prices{120.0, 250.0, 80.0, 999.0, 45.0, 400.0};
  std::vector<std::int32_t> quantities{3, 5, 2, 2, 1, 3};

  table.appendBatch(dpe::make_batch<sales_schema>(
      std::span{order_ids}, std::span{categories}, std::span{prices}, std::span{quantities}));
}

template <typename Result>
auto find_category_row(const Result& result, std::string_view category) -> std::size_t {
  for (std::size_t row = 0U; row < result.row_count(); ++row) {
    if (result.value(dpe::name_v<"category">, row) == category) {
      return row;
    }
  }
  throw test_failure{"missing category in grouped result"};
}

void test_schema_meta() {
  static_assert(sales_schema::column_count == 4U);
  static_assert(sales_schema::template index_of<"category">() == 1U);
  static_assert(std::same_as<dpe::column_type_t<"price", sales_schema>, double>);
}

void test_heap_pipeline() {
  dpe::table<sales_schema> sales;
  load_sales_data(sales);

  const auto revenue = dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">());
  const auto filtered =
      sales
          .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
          .select(dpe::as<"category">(dpe::col<"category">()), revenue)
          .evaluate();

  expect(filtered.row_count() == 3U, "expected three high-value rows");

  const auto grouped =
      sales
          .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
          .select(dpe::as<"category">(dpe::col<"category">()), revenue)
          .groupBy(dpe::name_v<"category">)
          .aggregate(dpe::count(dpe::name_v<"orders">),
                     dpe::sum(dpe::name_v<"revenue">, dpe::name_v<"gross_revenue">),
                     dpe::avg(dpe::name_v<"revenue">, dpe::name_v<"avg_revenue">));

  expect(grouped.row_count() == 2U, "expected two categories in grouped result");

  const auto hardware_row = find_category_row(grouped, "hardware");
  expect(grouped.value(dpe::name_v<"orders">, hardware_row) == 1, "hardware count mismatch");
  expect(std::fabs(grouped.value(dpe::name_v<"gross_revenue">, hardware_row) - 1250.0) < 1e-9,
         "hardware gross revenue mismatch");
  expect(std::fabs(grouped.value(dpe::name_v<"avg_revenue">, hardware_row) - 1250.0) < 1e-9,
         "hardware avg revenue mismatch");

  const auto software_row = find_category_row(grouped, "software");
  expect(grouped.value(dpe::name_v<"orders">, software_row) == 2, "software count mismatch");
  expect(std::fabs(grouped.value(dpe::name_v<"gross_revenue">, software_row) - 3198.0) < 1e-9,
         "software gross revenue mismatch");
  expect(std::fabs(grouped.value(dpe::name_v<"avg_revenue">, software_row) - 1599.0) < 1e-9,
         "software avg revenue mismatch");
}

void test_string_filter_and_projection() {
  dpe::table<sales_schema> sales;
  load_sales_data(sales);

  const auto result =
      sales
          .filter(DPE_COL("category") == "software")
          .select(dpe::as(dpe::name_v<"order_id">, dpe::col(dpe::name_v<"order_id">)),
                  dpe::as(dpe::name_v<"category">, DPE_COL("category")),
                  dpe::as(dpe::name_v<"revenue">, DPE_COL("price") * DPE_COL("quantity")))
          .evaluate();

  expect(result.row_count() == 2U, "expected two software rows");
  expect(result.value(dpe::name_v<"order_id">, 0U) == 1004, "unexpected first software order");
  expect(result.value(dpe::name_v<"order_id">, 1U) == 1006, "unexpected second software order");
  expect(result.value(dpe::name_v<"category">, 0U) == "software",
         "string predicate should preserve the category");
  expect(std::fabs(result.value(dpe::name_v<"revenue">, 0U) - 1998.0) < 1e-9,
         "first software revenue mismatch");
  expect(std::fabs(result.value(dpe::name_v<"revenue">, 1U) - 1200.0) < 1e-9,
         "second software revenue mismatch");
}

void test_batch_validation() {
  std::vector<std::int64_t> order_ids{1001, 1002, 1003};
  std::vector<std::string_view> categories{"books", "hardware"};
  std::vector<double> prices{120.0, 250.0, 80.0};
  std::vector<std::int32_t> quantities{3, 5, 2};

  bool threw = false;
  try {
    [[maybe_unused]] auto invalid = dpe::make_batch<sales_schema>(
        std::span{order_ids}, std::span{categories}, std::span{prices}, std::span{quantities});
  } catch (const std::invalid_argument&) {
    threw = true;
  }

  expect(threw, "mismatched batch column sizes must throw");
}

void test_lazy_relation_snapshot_boundary() {
  using event_schema = dpe::schema<dpe::field<"value", std::int64_t>>;

  dpe::table<event_schema> events;

  std::vector<std::int64_t> first_batch{1, 2, 3};
  events.appendBatch(dpe::make_batch<event_schema>(std::span{first_batch}));

  const auto frozen_relation =
      events
          .filter(dpe::col(dpe::name_v<"value">) >= 2)
          .select(dpe::as<"scaled">(dpe::col(dpe::name_v<"value">) * 10));

  std::vector<std::int64_t> second_batch{4, 5, 6};
  events.appendBatch(dpe::make_batch<event_schema>(std::span{second_batch}));

  const auto frozen_result = frozen_relation.evaluate();
  expect(frozen_result.row_count() == 2U, "captured relations must evaluate against their snapshot");
  expect(frozen_result.value(dpe::name_v<"scaled">, 0U) == 20,
         "captured relation should see the original second row");
  expect(frozen_result.value(dpe::name_v<"scaled">, 1U) == 30,
         "captured relation should see the original third row");

  const auto fresh_result =
      events
          .filter(dpe::col(dpe::name_v<"value">) >= 2)
          .select(dpe::as<"scaled">(dpe::col(dpe::name_v<"value">) * 10))
          .evaluate();

  expect(fresh_result.row_count() == 5U, "fresh relations should observe newly published rows");
  expect(fresh_result.value(dpe::name_v<"scaled">, 4U) == 60,
         "fresh relation should include appended rows");
}

void test_snapshot_consistency() {
  using event_schema = dpe::schema<dpe::field<"value", std::int64_t>>;

  dpe::table<event_schema> events;
  auto initial_snapshot = events.snapshot();

  std::thread writer{[&] {
    for (std::int64_t batch_index = 0; batch_index < 10; ++batch_index) {
      std::vector<std::int64_t> values(64U);
      for (std::size_t row = 0U; row < values.size(); ++row) {
        values[row] = batch_index * 64 + static_cast<std::int64_t>(row);
      }
      events.appendBatch(dpe::make_batch<event_schema>(std::span{values}));
    }
  }};

  std::thread reader{[&] {
    std::size_t last_seen = 0U;
    for (std::size_t iteration = 0U; iteration < 100U; ++iteration) {
      auto snapshot = events.snapshot();
      expect(snapshot->row_count() >= last_seen, "snapshot row count must be monotonic");
      last_seen = snapshot->row_count();
      std::this_thread::yield();
    }
  }};

  writer.join();
  reader.join();

  expect(initial_snapshot->row_count() == 0U, "published snapshots must remain immutable");
  expect(events.snapshot()->row_count() == 640U, "final appended row count mismatch");
}

void test_mmap_parity() {
  const auto temp_root =
      std::filesystem::temp_directory_path() / "dpe-mmap-runtime-test";
  std::filesystem::remove_all(temp_root);

  dpe::table<sales_schema> heap_table;
  dpe::table<sales_schema, dpe::mmap_storage_policy> mmap_table{
      dpe::mmap_storage_policy{temp_root}};

  load_sales_data(heap_table);
  load_sales_data(mmap_table);

  const auto heap_result = heap_table.evaluate();
  const auto mmap_result = mmap_table.evaluate();

  expect(heap_result.row_count() == mmap_result.row_count(), "heap/mmap row count mismatch");

  for (std::size_t row = 0U; row < heap_result.row_count(); ++row) {
    expect(heap_result.value(dpe::name_v<"order_id">, row) ==
               mmap_result.value(dpe::name_v<"order_id">, row),
           "heap/mmap order_id mismatch");
    expect(heap_result.value(dpe::name_v<"category">, row) ==
               mmap_result.value(dpe::name_v<"category">, row),
           "heap/mmap category mismatch");
    expect(std::fabs(heap_result.value(dpe::name_v<"price">, row) -
                     mmap_result.value(dpe::name_v<"price">, row)) < 1e-9,
           "heap/mmap price mismatch");
    expect(heap_result.value(dpe::name_v<"quantity">, row) ==
               mmap_result.value(dpe::name_v<"quantity">, row),
           "heap/mmap quantity mismatch");
  }
}

void test_parallel_matches_sequenced() {
  dpe::table<sales_schema, dpe::heap_storage_policy, dpe::sequenced_execution> sequenced;
  dpe::table<sales_schema, dpe::heap_storage_policy, dpe::parallel_execution> parallel{
      dpe::heap_storage_policy{}, dpe::parallel_execution{4U}};

  load_sales_data(sequenced);
  load_sales_data(parallel);

  const auto seq_result =
      sequenced
          .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
          .select(dpe::as<"category">(dpe::col<"category">()),
                  dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">()))
          .groupBy(dpe::name_v<"category">)
          .aggregate(dpe::count<"orders">(), dpe::sum<"revenue", "gross_revenue">());

  const auto par_result =
      parallel
          .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
          .select(dpe::as<"category">(dpe::col<"category">()),
                  dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">()))
          .groupBy(dpe::name_v<"category">)
          .aggregate(dpe::count<"orders">(), dpe::sum<"revenue", "gross_revenue">());

  expect(seq_result.row_count() == par_result.row_count(), "parallel grouped result row mismatch");

  const auto seq_software = find_category_row(seq_result, "software");
  const auto par_software = find_category_row(par_result, "software");

  expect(seq_result.value(dpe::name_v<"orders">, seq_software) ==
             par_result.value(dpe::name_v<"orders">, par_software),
         "parallel count mismatch");
  expect(std::fabs(seq_result.value(dpe::name_v<"gross_revenue">, seq_software) -
                   par_result.value(dpe::name_v<"gross_revenue">, par_software)) < 1e-9,
         "parallel revenue mismatch");
}

}  // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"schema_meta", test_schema_meta},
      {"heap_pipeline", test_heap_pipeline},
      {"string_filter_and_projection", test_string_filter_and_projection},
      {"batch_validation", test_batch_validation},
      {"lazy_relation_snapshot_boundary", test_lazy_relation_snapshot_boundary},
      {"snapshot_consistency", test_snapshot_consistency},
      {"mmap_parity", test_mmap_parity},
      {"parallel_matches_sequenced", test_parallel_matches_sequenced},
  };

  try {
    for (const auto& [name, test] : tests) {
      test();
      std::cout << "[pass] " << name << '\n';
    }
  } catch (const std::exception& error) {
    std::cerr << "[fail] " << error.what() << '\n';
    return 1;
  }

  return 0;
}
