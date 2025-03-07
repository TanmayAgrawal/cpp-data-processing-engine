#include <cstdint>

#include "dpe/dpe.hpp"

using duplicate_projection_schema =
    dpe::schema<dpe::field<"price", double>, dpe::field<"quantity", std::int32_t>>;

int main() {
  dpe::table<duplicate_projection_schema> table;
  auto projected = table.select(dpe::as<"metric">(dpe::col<"price">()),
                                dpe::as<"metric">(dpe::col<"quantity">()));
  (void)projected;
  return 0;
}
