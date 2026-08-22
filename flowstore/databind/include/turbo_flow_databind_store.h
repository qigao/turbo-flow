#ifndef TURBO_FLOW_DATABIND_STORE_H
#define TURBO_FLOW_DATABIND_STORE_H

#include "turbo_flow_store.h"

#include "data_bind.h"
#include "query_vm.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DATABIND_BINDING_ABI_VERSION 1u

typedef struct turbo_flow_databind_binding_s turbo_flow_databind_binding_t;

/**
 * Owns one DataBind schema/type interpreter. It owns no FlowStore and has no backend dependency.
 */
typedef struct turbo_flow_databind_binding_config_s {
  size_t size;
  uint32_t abi_version;
  const char *schema_text;
  size_t schema_size;
  const char *type_name;
  qvm_limits_t query_limits;
} turbo_flow_databind_binding_config_t;

#define TURBO_FLOW_DATABIND_BINDING_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_databind_binding_config_t), TURBO_FLOW_DATABIND_BINDING_ABI_VERSION, NULL,  \
   0u, NULL, {QVM_DEFAULT_MAX_INSTRUCTIONS, QVM_DEFAULT_MAX_OPERANDS,                            \
              QVM_DEFAULT_MAX_REGEXES, QVM_DEFAULT_MAX_STEPS}}

/** Borrowed decoded view; valid only for the enclosing DataBind scan/query callback. */
typedef struct turbo_flow_databind_view_s {
  size_t size;
  const uint8_t *key;
  size_t key_size;
  uint64_t revision;
  const DataBindRecord *record;
} turbo_flow_databind_view_t;

#define TURBO_FLOW_DATABIND_VIEW_INIT {sizeof(turbo_flow_databind_view_t), NULL, 0u, 0u, NULL}

typedef int (*turbo_flow_databind_visit_fn)(void *ctx, const turbo_flow_databind_view_t *record);

typedef enum turbo_flow_databind_operand_kind_e {
  TURBO_FLOW_DATABIND_OPERAND_FIELD = 1,
  TURBO_FLOW_DATABIND_OPERAND_BOOL,
  TURBO_FLOW_DATABIND_OPERAND_INT64,
  TURBO_FLOW_DATABIND_OPERAND_UINT64,
  TURBO_FLOW_DATABIND_OPERAND_DOUBLE,
  TURBO_FLOW_DATABIND_OPERAND_STRING
} turbo_flow_databind_operand_kind_t;

/** FIELD names are canonical, top-level DataBind field names. */
typedef struct turbo_flow_databind_operand_s {
  size_t size;
  turbo_flow_databind_operand_kind_t kind;
  union {
    const char *field_name;
    int boolean;
    int64_t integer;
    uint64_t uinteger;
    double number;
    struct {
      const char *data;
      size_t size;
    } string;
  } value;
} turbo_flow_databind_operand_t;

#define TURBO_FLOW_DATABIND_OPERAND_INIT                                                          \
  {sizeof(turbo_flow_databind_operand_t), TURBO_FLOW_DATABIND_OPERAND_FIELD, {.field_name = NULL}}

typedef enum turbo_flow_databind_compare_e {
  TURBO_FLOW_DATABIND_COMPARE_EQ = 0,
  TURBO_FLOW_DATABIND_COMPARE_NE = 1,
  TURBO_FLOW_DATABIND_COMPARE_LT = 2,
  TURBO_FLOW_DATABIND_COMPARE_LE = 3,
  TURBO_FLOW_DATABIND_COMPARE_GT = 4,
  TURBO_FLOW_DATABIND_COMPARE_GE = 5
} turbo_flow_databind_compare_t;

/** Caller-owned, precompiled QueryVM predicate. */
typedef struct turbo_flow_databind_query_s {
  size_t size;
  const qvm_instruction_t *instructions;
  uint32_t instruction_count;
  uint32_t offset;
  uint32_t length;
  const turbo_flow_databind_operand_t *operands;
  uint32_t operand_count;
  uint32_t regex_count;
} turbo_flow_databind_query_t;

#define TURBO_FLOW_DATABIND_QUERY_INIT {sizeof(turbo_flow_databind_query_t), NULL, 0u, 0u, 0u, NULL, 0u, 0u}

TURBO_FLOW_C_API int turbo_flow_databind_binding_create(
    const turbo_flow_databind_binding_config_t *config, turbo_flow_databind_binding_t **out);
TURBO_FLOW_C_API void turbo_flow_databind_binding_destroy(turbo_flow_databind_binding_t *binding);

/** Serialize a matching DataBind record then atomically write its binary Record representation. */
TURBO_FLOW_C_API int turbo_flow_databind_put(turbo_flow_store_t *store,
                                      turbo_flow_databind_binding_t *binding,
                                      const uint8_t *key, size_t key_size,
                                      uint64_t expected_revision, uint64_t next_revision,
                                      const DataBindRecord *record);

/** Decode one binary Record into a caller-owned DataBind record. */
TURBO_FLOW_C_API int turbo_flow_databind_get(turbo_flow_store_t *store,
                                      turbo_flow_databind_binding_t *binding,
                                      const uint8_t *key, size_t key_size,
                                      DataBindRecord **out, uint64_t *revision);

/** Decode and visit all binary Records in the FlowStore snapshot. */
TURBO_FLOW_C_API int turbo_flow_databind_scan(turbo_flow_store_t *store,
                                       turbo_flow_databind_binding_t *binding,
                                       turbo_flow_databind_visit_fn visit, void *ctx);

/**
 * Query decoded DataBind records using a verified QueryVM predicate. Only LOAD_PATH, LOAD_CONST,
 * LOAD_INVALID, EXISTS, NOT_EXISTS, LENGTH, COUNT, CMP, NOT, forward JMP variants, TRUE, FALSE,
 * and SELECT are accepted; other valid VM opcodes return TURBO_ENOTSUP before scanning.
 */
TURBO_FLOW_C_API int turbo_flow_databind_query(turbo_flow_store_t *store,
                                        turbo_flow_databind_binding_t *binding,
                                        const turbo_flow_databind_query_t *query,
                                        turbo_flow_databind_visit_fn visit, void *ctx,
                                        size_t *matched);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DATABIND_STORE_H */
