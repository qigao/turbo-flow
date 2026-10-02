#ifndef TURBO_FLOW_TURBODB_DURABLE_INTERNAL_H
#define TURBO_FLOW_TURBODB_DURABLE_INTERNAL_H

#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_provider.h"
#include "turbo_flow_turbodb.h"
#include "turbo_flow_turbodb_resource.h"
#include "turbodb_provider_config_native.h"

#define FLOW_DURABLE_TURBODB_KIND "flow.durable.turbodb"

typedef struct durable_turbodb_config_s {
  turbo_flow_turbodb_inbox_config_t inbox;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
} durable_turbodb_config_t;

int durable_turbodb_config_from_typed(
    const DurableTurboDbConfig_t *typed, const char *name,
    durable_turbodb_config_t *out, turbo_flow_config_error_t *error);

#endif
