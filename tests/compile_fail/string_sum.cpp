#include <cstdint>

#include "dpe/dpe.hpp"

using demo_schema = dpe::schema<dpe::field<"category", std::string>>;

int main() {
  dpe::table<demo_schema> table;
  [[maybe_unused]] auto result = table.aggregate(dpe::sum<"category", "total_category">());
  return 0;
}
