#include <cstdint>

#include "dpe/dpe.hpp"

using invalid_schema = dpe::schema<
    dpe::field<"id", std::int32_t>,
    dpe::field<"id", std::int64_t>>;

int main() {
  [[maybe_unused]] invalid_schema schema_instance{};
  return 0;
}
