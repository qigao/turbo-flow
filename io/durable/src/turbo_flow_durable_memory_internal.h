#ifndef TURBO_FLOW_DURABLE_MEMORY_INTERNAL_H
#define TURBO_FLOW_DURABLE_MEMORY_INTERNAL_H

#include "durable_memory_provider_config_native.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_durable_memory_resource.h"
#include "turbo_flow_provider.h"

#define FLOW_DURABLE_MEMORY_KIND "flow.durable.memory"
#define FLOW_DURABLE_MEMORY_RESOURCE_EXPORT_ID "flow.durable.memory.resource"

typedef struct durable_memory_config_s {
  turbo_flow_inbox_memory_config_t memory;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
} durable_memory_config_t;

int durable_memory_config_from_typed(
    const DurableMemoryConfig_t *typed,
    const turbo_flow_durable_memory_resource_view_t *resource,
    const char *name,
    durable_memory_config_t *out,
    turbo_flow_config_error_t *error);

#endif
