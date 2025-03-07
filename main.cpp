#include <cstdint>
#include <iostream>
#include <type_traits>

#include "dpe/dpe.hpp"

namespace {

using foundation_schema = dpe::schema<
    dpe::field<"order_id", std::int64_t>,
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

}  // namespace

int main() {
  [[maybe_unused]] const auto revenue = dpe::col<"price">() * dpe::col<"quantity">();
  [[maybe_unused]] const auto high_value = revenue > 1000.0;

  static_assert(foundation_schema::column_count == 3U);
  static_assert(foundation_schema::template index_of<"price">() == 1U);
  static_assert(std::same_as<dpe::column_type_t<"quantity", foundation_schema>, std::int32_t>);
  static_assert(dpe::well_formed_expression_for<decltype(revenue), foundation_schema>);
  static_assert(dpe::boolean_expression_for<decltype(high_value), foundation_schema>);

  std::cout
      << "DPE foundation: compile-time schema metadata and lazy expression templates are ready.\n";

  return 0;
}
