#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <span>
#include <thread>
#include <vector>

#include "dpe/dpe.hpp"

namespace {

using sales_schema = dpe::schema<
    dpe::field<"order_id", std::int64_t>,
    dpe::field<"category", std::string>,
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

}  // namespace

int main() {
  dpe::table<sales_schema, dpe::heap_storage_policy, dpe::parallel_execution> sales{
      dpe::heap_storage_policy{},
      dpe::parallel_execution{std::max<std::size_t>(1U, std::thread::hardware_concurrency())}};

  std::vector<std::int64_t> order_ids{1001, 1002, 1003, 1004, 1005, 1006};
  std::vector<std::string_view> categories{
      "books", "hardware", "books", "software", "hardware", "software"};
  std::vector<double> prices{120.0, 250.0, 80.0, 999.0, 45.0, 400.0};
  std::vector<std::int32_t> quantities{3, 5, 2, 2, 1, 3};

  sales.appendBatch(dpe::make_batch<sales_schema>(
      std::span{order_ids}, std::span{categories}, std::span{prices}, std::span{quantities}));

  const auto revenue = dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">());

  const auto summary =
      sales
          .filter((dpe::col<"price">() * dpe::col<"quantity">()) > 1000.0)
          .select(dpe::as<"category">(dpe::col<"category">()), revenue)
          .groupBy(dpe::name_v<"category">)
          .aggregate(dpe::count<"orders">(), dpe::sum<"revenue", "gross_revenue">(),
                     dpe::avg<"revenue", "avg_revenue">());

  std::cout << "High-value order summary\n";
  std::cout << "========================\n";
  std::cout << std::left << std::setw(12) << "Category" << std::right << std::setw(10)
            << "Orders" << std::setw(18) << "Gross Revenue" << std::setw(16)
            << "Avg Revenue" << '\n';

  for (std::size_t row = 0U; row < summary.row_count(); ++row) {
    std::cout << std::left << std::setw(12) << summary.value(dpe::name_v<"category">, row)
              << std::right << std::setw(10) << summary.value(dpe::name_v<"orders">, row)
              << std::setw(18)
              << std::fixed << std::setprecision(2)
              << summary.value(dpe::name_v<"gross_revenue">, row) << std::setw(16)
              << summary.value(dpe::name_v<"avg_revenue">, row) << '\n';
  }

  return 0;
}
