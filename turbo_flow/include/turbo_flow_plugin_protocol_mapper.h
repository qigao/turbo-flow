#ifndef TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_H
#define TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_H

#include "turbo_flow_plugin_abi.h"
#include "turbo_flow_protocol_mapper.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
  TURBO_FLOW_PLUGIN_CAP_PROTOCOL_MAPPER = UINT64_C(1) << 10
};

typedef int (*turbo_flow_plugin_add_protocol_mapper_fn)(
    void *ctx, const turbo_flow_protocol_mapper_v1_t *mapper);

typedef struct turbo_flow_plugin_protocol_mapper_catalog_entry_v1_s {
  const char *plugin_id;
  turbo_flow_protocol_mapper_v1_t mapper;
} turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t;

typedef struct turbo_flow_plugin_protocol_mapper_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t *entries;
  size_t count;
} turbo_flow_plugin_protocol_mapper_catalog_v1_t;

#define TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_CATALOG_V1_INIT                                         \
  {sizeof(turbo_flow_plugin_protocol_mapper_catalog_v1_t),                                        \
   TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR, TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, NULL, 0u}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PLUGIN_PROTOCOL_MAPPER_H */
