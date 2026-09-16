#ifndef TURBO_FLOW_DURABLE_MEMORY_INTERNAL_H
#define TURBO_FLOW_DURABLE_MEMORY_INTERNAL_H
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#define FLOW_DURABLE_MEMORY_KIND "flow.durable.memory"
typedef struct durable_memory_config_s {
  turbo_flow_inbox_memory_config_t memory;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
} durable_memory_config_t;
int durable_memory_config_read(const turbo_flow_resolved_config_t *resolved, const char *name,
                               durable_memory_config_t *out, turbo_flow_config_error_t *error);
#endif
