#include "dpe/dpe.hpp"

using duplicate_aggregate_schema =
    dpe::schema<dpe::field<"category", std::string>, dpe::field<"price", double>>;

int main() {
  dpe::table<duplicate_aggregate_schema> table;
  auto result =
      table.groupBy(dpe::name_v<"category">)
          .aggregate(dpe::count<"category">(), dpe::sum<"price", "gross_revenue">());
  (void)result;
  return 0;
}
