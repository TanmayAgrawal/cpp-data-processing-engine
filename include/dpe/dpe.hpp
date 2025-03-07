#pragma once

#include "dpe/aggregate.hpp"
#include "dpe/batch.hpp"
#include "dpe/concepts.hpp"
#include "dpe/execution.hpp"
#include "dpe/expression.hpp"
#include "dpe/fixed_string.hpp"
#include "dpe/schema.hpp"
#include "dpe/storage.hpp"
#include "dpe/table.hpp"

#ifndef DPE_COL
#define DPE_COL(name_literal) ::dpe::col<::dpe::fixed_string{name_literal}>()
#endif
