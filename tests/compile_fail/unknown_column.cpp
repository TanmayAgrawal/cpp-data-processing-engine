#include <cstdint>

#include "dpe/dpe.hpp"

using demo_schema = dpe::schema<dpe::field<"id", std::int32_t>>;

int main() {
  dpe::table<demo_schema> table;
  [[maybe_unused]] auto relation = table.filter(dpe::col<"missing">() > 0);
  return 0;
}
