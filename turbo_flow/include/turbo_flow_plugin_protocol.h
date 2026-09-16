#ifndef TURBO_FLOW_PLUGIN_PROTOCOL_H
#define TURBO_FLOW_PLUGIN_PROTOCOL_H

#include "turbo_flow_plugin.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_business.h"

#ifdef __cplusplus
extern "C" {
#endif

struct turbo_flow_plugin_protocol_provider_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_protocol_plugin_api_t provider;
};

#define TURBO_FLOW_PLUGIN_PROTOCOL_PROVIDER_V1_INIT                                                \
  {sizeof(turbo_flow_plugin_protocol_provider_v1_t),                                               \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   {0}}

struct turbo_flow_plugin_business_provider_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_protocol_business_plugin_api_t provider;
};

#define TURBO_FLOW_PLUGIN_BUSINESS_PROVIDER_V1_INIT                                                \
  {sizeof(turbo_flow_plugin_business_provider_v1_t),                                               \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   {0}}

typedef struct turbo_flow_plugin_protocol_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_protocol_plugin_api_t *protocol_providers;
  size_t protocol_provider_count;
  const turbo_flow_protocol_business_plugin_api_t *business_providers;
  size_t business_provider_count;
} turbo_flow_plugin_protocol_catalog_v1_t;

#define TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT                                                 \
  {sizeof(turbo_flow_plugin_protocol_catalog_v1_t),                                                \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                            \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                            \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u}

/** Fill immutable Protocol/Business arrays borrowed until the snapshot's final release. */
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_protocol_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_protocol_catalog_v1_t *catalog_out);

/** Create registries that retain snapshot ownership until their successful destruction. */
TURBO_FLOW_C_API int
turbo_flow_protocol_registry_create(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                    turbo_flow_protocol_registry_t **out);
TURBO_FLOW_C_API int
turbo_flow_protocol_business_registry_create(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                             turbo_flow_protocol_business_registry_t **out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PLUGIN_PROTOCOL_H */
