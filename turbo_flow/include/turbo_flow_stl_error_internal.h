#ifndef TURBO_FLOW_STL_ERROR_INTERNAL_H
#define TURBO_FLOW_STL_ERROR_INTERNAL_H

#include "turbo_error.h"

#include <turbostl/deque.h>
#include <turbostl/hash_map.h>
#include <turbostl/hash_set.h>
#include <turbostl/status.h>
#include <turbostl/vec.h>

#include <stddef.h>
#include <stdint.h>

typedef union turbo_flow_max_align_u {
  long double long_double_value;
  void *pointer_value;
  long long integer_value;
} turbo_flow_max_align_t;

static inline int turbo_flow_stl_error(stl_status status) {
  switch (status) {
  case STL_OK:
    return TURBO_OK;
  case STL_INVALID_ARGUMENT:
    return TURBO_EINVAL;
  case STL_OUT_OF_MEMORY:
    return TURBO_ENOMEM;
  case STL_CAPACITY_EXCEEDED:
    return TURBO_ENOSPC;
  case STL_EMPTY:
  case STL_NOT_FOUND:
    return TURBO_ENOENT;
  case STL_TYPE_MISMATCH:
  case STL_TRAIT_MISSING:
  default:
    return TURBO_EPROTO;
  }
}

#endif /* TURBO_FLOW_STL_ERROR_INTERNAL_H */
