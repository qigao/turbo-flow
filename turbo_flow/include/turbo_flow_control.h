#ifndef TURBO_FLOW_CONTROL_H
#define TURBO_FLOW_CONTROL_H

#include "turbo_flow_expr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Optional host snapshot facts used by one control evaluation.
 *
 * Schema paths and callback backing storage are borrowed for the duration of
 * turbo_flow_control_execute_ex(). Providers must expose immutable snapshot
 * values and must not perform control side effects from read_field.
 */
typedef struct turbo_flow_control_facts_s {
  size_t size;
  const turbo_flow_expr_schema_t *schema;
  turbo_flow_expr_read_schema_field_fn read_field;
  void *ctx;
} turbo_flow_control_facts_t;

#define TURBO_FLOW_CONTROL_FACTS_INIT \
  {sizeof(turbo_flow_control_facts_t), NULL, NULL, NULL}

/** Execute a parsed command with optional host-owned typed snapshot facts. */
TURBO_FLOW_C_API int turbo_flow_control_execute_ex(turbo_flow_t *flow,
                                            const turbo_flow_control_command_t *command,
                                            const turbo_flow_control_facts_t *facts,
                                            turbo_flow_error_t *error);

/** Parse and execute one command with optional host-owned typed snapshot facts. */
TURBO_FLOW_C_API int turbo_flow_control_ex(turbo_flow_t *flow, const char *text, size_t len,
                                    const turbo_flow_control_facts_t *facts,
                                    turbo_flow_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CONTROL_H */
