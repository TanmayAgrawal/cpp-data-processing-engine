#pragma once

#include "dpe/batch.hpp"
#include "dpe/concepts.hpp"
#include "dpe/expression.hpp"
#include "dpe/fixed_string.hpp"
#include "dpe/schema.hpp"

#ifndef DPE_COL
#define DPE_COL(name_literal) ::dpe::col<::dpe::fixed_string{name_literal}>()
#endif
