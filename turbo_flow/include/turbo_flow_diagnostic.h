#ifndef TURBO_FLOW_DIAGNOSTIC_H
#define TURBO_FLOW_DIAGNOSTIC_H

#include "salts_error.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DIAGNOSTIC_PATH_MAX 255u
#define TURBO_FLOW_DIAGNOSTIC_MESSAGE_MAX 255u

/**
 * Generic structured control-plane diagnostic.
 *
 * This type is not owned by the legacy YAML/resolved-config subsystem. It is
 * shared by compile/preflight/provider/resource/control APIs that need a stable
 * path + status + message result.
 */
typedef struct turbo_flow_config_error_s {
  size_t size;
  int status;
  char path[TURBO_FLOW_DIAGNOSTIC_PATH_MAX + 1u];
  char message[TURBO_FLOW_DIAGNOSTIC_MESSAGE_MAX + 1u];
} turbo_flow_config_error_t;

#define TURBO_FLOW_CONFIG_ERROR_INIT \
  {sizeof(turbo_flow_config_error_t), SALTS_OK, {0}, {0}}

/* Transitional source compatibility for code that used the old constant names.
 * The aliases describe the same generic diagnostic ABI and disappear when the
 * old Config public surface is removed under #241. */
#define TURBO_FLOW_CONFIG_PATH_MAX TURBO_FLOW_DIAGNOSTIC_PATH_MAX
#define TURBO_FLOW_CONFIG_MESSAGE_MAX TURBO_FLOW_DIAGNOSTIC_MESSAGE_MAX

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DIAGNOSTIC_H */
