#ifndef TURBO_FLOW_PLUGIN_MATERIALIZER_H
#define TURBO_FLOW_PLUGIN_MATERIALIZER_H

#include "turbo_flow_export.h"
#include "turbo_flow.h"
#include "turbo_flow_plugin_abi.h"

#include <cmeta/data.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
  TURBO_FLOW_PLUGIN_CAP_MATERIALIZER = UINT64_C(1) << 9,
  TURBO_FLOW_PLUGIN_MATERIALIZER_THREAD_SAFE = 1u,
  TURBO_FLOW_PLUGIN_MATERIALIZER_CALLER_BUFFER = 1u,
  TURBO_FLOW_PLUGIN_MATERIALIZER_MAX_BYTES = 67108864u
};

/**
 * One canonical encoded payload passed to a compiled materializer.
 *
 * The encoded bytes are borrowed for the callback only. Generation/runtime code
 * validates the configured schema/version/encoding before invoking a materializer;
 * the callback therefore only decodes the already-selected canonical format.
 */
typedef struct turbo_flow_plugin_materializer_input_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const void *data;
  size_t data_size;
} turbo_flow_plugin_materializer_input_v1_t;

#define TURBO_FLOW_PLUGIN_MATERIALIZER_INPUT_V1_INIT                                                 {sizeof(turbo_flow_plugin_materializer_input_v1_t),                                                 TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR, TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, NULL, 0u}

/**
 * Decode one bounded canonical payload into caller-owned native storage.
 *
 * v1 never transfers heap ownership across the DLL boundary. native_out points
 * to host-owned writable storage with exactly the descriptor's native_bytes
 * capacity. The registered CMeta storage alignment must not exceed max_align_t;
 * over-aligned native layouts are rejected by PluginHost. A callback must not
 * retain encoded input or native_out.
 */
typedef int (*turbo_flow_plugin_materialize_fn)(
    void *ctx, const turbo_flow_plugin_materializer_input_v1_t *input,
    void *native_out, size_t native_capacity);

/**
 * Immutable schema materializer descriptor copied by PluginHost.
 *
 * schema strings, data metadata and callback code remain DLL-owned and must stay
 * immutable while a catalog snapshot or generation lease references the module.
 * v1 supports only fixed-size caller-buffer output and thread-safe callbacks.
 */
typedef struct turbo_flow_plugin_materializer_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_data_schema_t schema;
  const cmeta_data_desc *data;
  size_t max_encoded_bytes;
  size_t native_bytes;
  uint32_t threading;
  uint32_t ownership;
  void *ctx;
  turbo_flow_plugin_materialize_fn materialize;
} turbo_flow_plugin_materializer_v1_t;

#define TURBO_FLOW_PLUGIN_MATERIALIZER_V1_INIT                                                       {sizeof(turbo_flow_plugin_materializer_v1_t),                                                       TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR, TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                           TURBO_FLOW_DATA_SCHEMA_INIT, NULL, 0u, 0u, 0u, 0u, NULL, NULL}

typedef int (*turbo_flow_plugin_add_materializer_fn)(
    void *ctx, const turbo_flow_plugin_materializer_v1_t *materializer);

typedef struct turbo_flow_plugin_materializer_catalog_entry_v1_s {
  const char *plugin_id;
  turbo_flow_plugin_materializer_v1_t materializer;
} turbo_flow_plugin_materializer_catalog_entry_v1_t;

typedef struct turbo_flow_plugin_materializer_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_materializer_catalog_entry_v1_t *entries;
  size_t count;
} turbo_flow_plugin_materializer_catalog_v1_t;

#define TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT                                               {sizeof(turbo_flow_plugin_materializer_catalog_v1_t),                                               TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR, TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR, NULL, 0u}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PLUGIN_MATERIALIZER_H */
