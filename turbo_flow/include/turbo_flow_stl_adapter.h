#ifndef TURBO_FLOW_STL_ADAPTER_H
#define TURBO_FLOW_STL_ADAPTER_H

#include "turbo_error.h"

#include <turbostl/deque.h>
#include <turbostl/hash_map.h>
#include <turbostl/hash_set.h>
#include <turbostl/heap.h>
#include <turbostl/vec.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef union turbo_flow_max_align_u {
  long double long_double_value;
  void *pointer_value;
  long long integer_value;
} turbo_flow_max_align_t;

/* TurboFlow historically stores trivial byte values in these containers and
 * exposes TURBO_E* failures internally. Keep that contract at this boundary. */
static inline int turbo_flow_stl_status(turbo_stl_status status) {
  switch (status) {
    case TURBO_STL_OK:
      return TURBO_OK;
    case TURBO_STL_INVALID_ARGUMENT:
      return TURBO_EINVAL;
    case TURBO_STL_OUT_OF_MEMORY:
      return TURBO_ENOMEM;
    case TURBO_STL_CAPACITY_EXCEEDED:
      return TURBO_ENOSPC;
    case TURBO_STL_EMPTY:
    case TURBO_STL_NOT_FOUND:
      return TURBO_ENOENT;
    case TURBO_STL_TYPE_MISMATCH:
    case TURBO_STL_TRAIT_MISSING:
    default:
      return TURBO_EPROTO;
  }
}

static inline int turbo_flow_vec_init_bytes(turbo_vec_t *vec, size_t elem_size) {
  if (!vec || elem_size == 0u) return TURBO_EINVAL;
  memset(vec, 0, sizeof(*vec));
  return turbo_flow_stl_status(
      turbo_vec_init_bytes(vec, elem_size, _Alignof(turbo_flow_max_align_t), SIZE_MAX));
}

static inline int turbo_flow_deque_init_bytes(turbo_deque_t *deque, size_t elem_size) {
  if (!deque || elem_size == 0u) return TURBO_EINVAL;
  memset(deque, 0, sizeof(*deque));
  return turbo_flow_stl_status(
      turbo_deque_init_bytes(deque, elem_size, _Alignof(turbo_flow_max_align_t), SIZE_MAX));
}

static inline int turbo_flow_hash_map_init_bytes(
    turbo_hash_map_t *map, size_t key_size, size_t value_size, turbo_hash_fn hash,
    turbo_hash_equal_fn equal, void *ctx) {
  if (!map || key_size == 0u || value_size == 0u) return TURBO_EINVAL;
  memset(map, 0, sizeof(*map));
  return turbo_flow_stl_status(turbo_hash_map_init_bytes(
      map, key_size, _Alignof(turbo_flow_max_align_t), value_size,
      _Alignof(turbo_flow_max_align_t), SIZE_MAX,
      hash ? hash : turbo_hash_bytes, equal ? equal : turbo_hash_key_equal, ctx));
}

static inline int turbo_flow_hash_set_init_bytes(
    turbo_hash_set_t *set, size_t key_size, turbo_hash_fn hash, turbo_hash_equal_fn equal,
    void *ctx) {
  if (!set || key_size == 0u) return TURBO_EINVAL;
  memset(set, 0, sizeof(*set));
  return turbo_flow_stl_status(turbo_hash_set_init_bytes(
      set, key_size, _Alignof(turbo_flow_max_align_t), SIZE_MAX,
      hash ? hash : turbo_hash_bytes,
      equal ? equal : turbo_hash_key_equal, ctx));
}

static inline int turbo_flow_heap_init_bytes(turbo_heap_t *heap, size_t elem_size,
                                             turbo_heap_compare_fn compare) {
  if (!heap || elem_size == 0u || !compare) return TURBO_EINVAL;
  memset(heap, 0, sizeof(*heap));
  return turbo_flow_stl_status(turbo_heap_init_bytes(
      heap, elem_size, _Alignof(turbo_flow_max_align_t), SIZE_MAX, compare, NULL));
}

#define TURBO_FLOW_VEC_DEFINE(name, type)                                                   \
  typedef struct name {                                                                     \
    turbo_vec_t raw;                                                                        \
  } name;                                                                                   \
  static inline int name##_init(name *self) {                                               \
    return turbo_flow_vec_init_bytes(&self->raw, sizeof(type));                             \
  }                                                                                         \
  static inline void name##_destroy(name *self) { turbo_vec_destroy(&self->raw); }          \
  static inline int name##_reserve(name *self, size_t count) {                              \
    return turbo_flow_stl_status(turbo_vec_reserve(&self->raw, count));                     \
  }                                                                                         \
  static inline int name##_push(name *self, type value) {                                   \
    return turbo_flow_stl_status(turbo_vec_push(&self->raw, &value));                       \
  }                                                                                         \
  static inline int name##_pop(name *self, type *out) {                                     \
    return turbo_vec_pop(&self->raw, out) == TURBO_STL_OK;                                  \
  }                                                                                         \
  static inline type *name##_at(name *self, size_t index) {                                 \
    return (type *)turbo_vec_at(&self->raw, index);                                         \
  }                                                                                         \
  static inline const type *name##_at_const(const name *self, size_t index) {               \
    return (const type *)turbo_vec_at_const(&self->raw, index);                             \
  }                                                                                         \
  static inline type *name##_data(name *self) { return (type *)turbo_vec_data(&self->raw); } \
  static inline const type *name##_data_const(const name *self) {                           \
    return (const type *)turbo_vec_data_const(&self->raw);                                  \
  }                                                                                         \
  static inline size_t name##_size(const name *self) { return turbo_vec_size(&self->raw); }

#define TURBO_FLOW_HASH_MAP_DEFINE(name, key_type, value_type)                              \
  typedef struct name {                                                                     \
    turbo_hash_map_t raw;                                                                   \
  } name;                                                                                   \
  static inline int name##_init(name *self) {                                               \
    return turbo_flow_hash_map_init_bytes(&self->raw, sizeof(key_type), sizeof(value_type), \
                                          NULL, NULL, NULL);                                 \
  }                                                                                         \
  static inline void name##_destroy(name *self) { turbo_hash_map_destroy(&self->raw); }     \
  static inline int name##_put(name *self, key_type key, value_type value) {                \
    return turbo_flow_stl_status(turbo_hash_map_put(&self->raw, &key, &value));             \
  }                                                                                         \
  static inline int name##_remove(name *self, key_type key, value_type *out) {              \
    return turbo_hash_map_remove(&self->raw, &key, out) == TURBO_STL_OK;                    \
  }                                                                                         \
  static inline const value_type *name##_get_const(const name *self, key_type key) {        \
    return (const value_type *)turbo_hash_map_get_const(&self->raw, &key);                  \
  }

#define TURBO_FLOW_HEAP_DEFINE(name, type, compare_fn)                                      \
  typedef struct name {                                                                     \
    turbo_heap_t raw;                                                                       \
  } name;                                                                                   \
  static inline int name##_init(name *self) {                                               \
    return turbo_flow_heap_init_bytes(&self->raw, sizeof(type), compare_fn);                \
  }                                                                                         \
  static inline void name##_destroy(name *self) { turbo_heap_destroy(&self->raw); }         \
  static inline int name##_push(name *self, type value) {                                   \
    return turbo_flow_stl_status(turbo_heap_push(&self->raw, &value));                      \
  }                                                                                         \
  static inline int name##_pop(name *self, type *out) {                                     \
    return turbo_heap_pop(&self->raw, out) == TURBO_STL_OK;                                 \
  }

/* Status-returning raw operations changed to a positive STL status domain. */
#define turbo_vec_init(vec, elem_size) turbo_flow_vec_init_bytes((vec), (elem_size))
#define turbo_vec_clear(vec) turbo_flow_stl_status(turbo_vec_clear((vec)))
#define turbo_vec_reserve(vec, count) turbo_flow_stl_status(turbo_vec_reserve((vec), (count)))
#define turbo_vec_resize(vec, count) turbo_flow_stl_status(turbo_vec_resize((vec), (count)))
#define turbo_vec_push(vec, elem) turbo_flow_stl_status(turbo_vec_push((vec), (elem)))
#define turbo_vec_pop(vec, out) turbo_flow_stl_status(turbo_vec_pop((vec), (out)))
#define turbo_vec_erase(vec, index, out) \
  turbo_flow_stl_status(turbo_vec_erase((vec), (index), (out)))
#define turbo_vec_swap_remove(vec, index, out) \
  turbo_flow_stl_status(turbo_vec_swap_remove((vec), (index), (out)))

#define turbo_deque_init(deque, elem_size) turbo_flow_deque_init_bytes((deque), (elem_size))
#define turbo_deque_clear(deque) turbo_flow_stl_status(turbo_deque_clear((deque)))
#define turbo_deque_reserve(deque, count) \
  turbo_flow_stl_status(turbo_deque_reserve((deque), (count)))
#define turbo_deque_push_back(deque, elem) \
  turbo_flow_stl_status(turbo_deque_push_back((deque), (elem)))
#define turbo_deque_pop_front(deque, out) \
  turbo_flow_stl_status(turbo_deque_pop_front((deque), (out)))

#define turbo_hash_map_init(map, key_size, value_size, hash, equal, ctx) \
  turbo_flow_hash_map_init_bytes((map), (key_size), (value_size), (hash), (equal), (ctx))
#define turbo_hash_map_reserve(map, count) \
  turbo_flow_stl_status(turbo_hash_map_reserve((map), (count)))
#define turbo_hash_map_put(map, key, value) \
  turbo_flow_stl_status(turbo_hash_map_put((map), (key), (value)))
#define turbo_hash_map_remove(map, key, out) \
  turbo_flow_stl_status(turbo_hash_map_remove((map), (key), (out)))

#define turbo_hash_set_init(set, key_size, hash, equal, ctx) \
  turbo_flow_hash_set_init_bytes((set), (key_size), (hash), (equal), (ctx))
#define turbo_hash_set_reserve(set, count) \
  turbo_flow_stl_status(turbo_hash_set_reserve((set), (count)))
#define turbo_hash_set_add(set, key) turbo_flow_stl_status(turbo_hash_set_add((set), (key)))
#define turbo_hash_set_remove(set, key) \
  turbo_flow_stl_status(turbo_hash_set_remove((set), (key)))

#define turbo_heap_reserve(heap, count) \
  turbo_flow_stl_status(turbo_heap_reserve((heap), (count)))

#endif /* TURBO_FLOW_STL_ADAPTER_H */
