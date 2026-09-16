#ifndef TURBO_FLOW_CONFIG_INTERNAL_H
#define TURBO_FLOW_CONFIG_INTERNAL_H

#include "turbo_flow_resolved_config.h"

#include <json_parser.h>

struct turbo_flow_resolved_config_s {
  json_value_t *document;
  char *json;
  size_t json_len;
};

int flow_config_error(turbo_flow_config_error_t *error, int status, const char *path,
                      const char *message);
int flow_config_object_keys(const json_value_t *object, const char *path,
                            const char *const *allowed, size_t count,
                            turbo_flow_config_error_t *error);
int flow_config_validate_operation_bindings(const json_value_t *bindings,
                                            const json_value_t *channels,
                                            turbo_flow_config_error_t *error);

#endif /* TURBO_FLOW_CONFIG_INTERNAL_H */
