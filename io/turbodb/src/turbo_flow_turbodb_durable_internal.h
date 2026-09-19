#ifndef TURBO_FLOW_TURBODB_DURABLE_INTERNAL_H
#define TURBO_FLOW_TURBODB_DURABLE_INTERNAL_H

#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_turbodb.h"

#define FLOW_DURABLE_TURBODB_KIND "flow.durable.turbodb"
#define FLOW_DURABLE_TURBODB_FILENAME_BYTES 4096u

typedef struct durable_turbodb_config_s {
  turbo_flow_turbodb_inbox_config_t inbox;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
  char filename[FLOW_DURABLE_TURBODB_FILENAME_BYTES];
  char namespace_name[TURBO_FLOW_TURBODB_INBOX_NAMESPACE_MAX + 1u];
} durable_turbodb_config_t;

int durable_turbodb_config_read(const turbo_flow_resolved_config_t *resolved, const char *name,
                                durable_turbodb_config_t *out,
                                turbo_flow_config_error_t *error);

#endif
