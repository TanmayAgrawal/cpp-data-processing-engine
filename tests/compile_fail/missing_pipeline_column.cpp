#include <cstdint>

#include "dpe/dpe.hpp"

using demo_schema = dpe::schema<
    dpe::field<"price", double>,
    dpe::field<"quantity", std::int32_t>>;

int main() {
  dpe::table<demo_schema> table;
  [[maybe_unused]] auto result =
      table.select(dpe::as<"revenue">(dpe::col<"price">() * dpe::col<"quantity">()))
          .aggregate(dpe::sum<"price", "sum_price">());
  return 0;
}
