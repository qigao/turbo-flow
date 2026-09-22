#ifndef TURBO_FLOW_PROTOCOL_MAPPER_H
#define TURBO_FLOW_PROTOCOL_MAPPER_H

#include "turbo_flow.h"
#include "turbo_flow_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION 1u
#define TURBO_FLOW_PROTOCOL_MAPPER_NAME_MAX 63u
#define TURBO_FLOW_PROTOCOL_MAPPER_PROFILE_MAX 63u

/**
 * Generation-time request for one exact pre-durable mapping contract.
 *
 * The host selects a mapper/provider before the data path and asks it to bind
 * one protocol/profile/message/semantic tuple to one canonical business
 * descriptor. No raw frame is exposed at this boundary.
 */
typedef struct turbo_flow_protocol_mapper_preflight_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  const char *profile;
  uint32_t message_type;
  uint32_t semantic_type;
  const char *semantic_media_type;
  size_t max_semantic_bytes;
  size_t max_output_bytes;
} turbo_flow_protocol_mapper_preflight_request_t;

#define TURBO_FLOW_PROTOCOL_MAPPER_PREFLIGHT_REQUEST_INIT                                         \
  {sizeof(turbo_flow_protocol_mapper_preflight_request_t),                                        \
   TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION,                                                        \
   TURBO_FLOW_PROTOCOL_MQTT_SN,                                                                   \
   NULL,                                                                                          \
   0u,                                                                                            \
   TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE,                                                        \
   NULL,                                                                                          \
   0u,                                                                                            \
   0u}

/**
 * Exact immutable business contract compiled during preflight.
 *
 * content must be a DATA-domain declared schema descriptor. The bounds are the
 * admitted maxima for this compiled mapper; the data path may only narrow them.
 */
typedef struct turbo_flow_protocol_mapper_contract_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  uint32_t message_type;
  uint32_t semantic_type;
  char profile[TURBO_FLOW_PROTOCOL_MAPPER_PROFILE_MAX + 1u];
  size_t max_semantic_bytes;
  size_t max_output_bytes;
  turbo_flow_content_descriptor_t content;
} turbo_flow_protocol_mapper_contract_t;

#define TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT                                                  \
  {sizeof(turbo_flow_protocol_mapper_contract_t),                                                  \
   TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION,                                                        \
   TURBO_FLOW_PROTOCOL_MQTT_SN,                                                                   \
   0u,                                                                                            \
   TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE,                                                        \
   {0},                                                                                           \
   0u,                                                                                            \
   0u,                                                                                            \
   TURBO_FLOW_CONTENT_DESCRIPTOR_INIT}

/**
 * Borrowed pre-durable mapper input.
 *
 * semantic bytes come only from turbo_flow_protocol_decode_semantic(); raw
 * frame bytes are intentionally absent from this contract. profile is the
 * configured mapping profile selected before data-path execution. All pointers
 * are borrowed for the synchronous map call and must not be retained.
 */
typedef struct turbo_flow_protocol_mapper_request_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_kind_t protocol;
  const char *profile;
  turbo_flow_protocol_metadata_t metadata;
  const uint8_t *semantic_data;
  size_t semantic_size;
  uint32_t semantic_type;
  const char *semantic_media_type;
} turbo_flow_protocol_mapper_request_t;

#define TURBO_FLOW_PROTOCOL_MAPPER_REQUEST_INIT                                                   \
  {sizeof(turbo_flow_protocol_mapper_request_t),                                                   \
   TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION,                                                        \
   TURBO_FLOW_PROTOCOL_MQTT_SN,                                                                   \
   NULL,                                                                                          \
   TURBO_FLOW_PROTOCOL_METADATA_INIT,                                                             \
   NULL,                                                                                          \
   0u,                                                                                            \
   TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE,                                                        \
   NULL}

/**
 * Caller-owned canonical business output.
 *
 * payload is supplied by the caller and must not be replaced or retained.
 * content must exactly match the compiled mapper contract before durable
 * admission. On failure payload_size remains zero and no partial business
 * object is admitted.
 */
typedef struct turbo_flow_protocol_mapper_output_s {
  size_t size;
  uint32_t abi_version;
  uint8_t *payload;
  size_t payload_capacity;
  size_t payload_size;
  turbo_flow_content_descriptor_t content;
} turbo_flow_protocol_mapper_output_t;

#define TURBO_FLOW_PROTOCOL_MAPPER_OUTPUT_INIT                                                    \
  {sizeof(turbo_flow_protocol_mapper_output_t),                                                   \
   TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION,                                                        \
   NULL,                                                                                          \
   0u,                                                                                            \
   0u,                                                                                            \
   TURBO_FLOW_CONTENT_DESCRIPTOR_INIT}

typedef int (*turbo_flow_protocol_mapper_preflight_fn)(
    void *ctx, const turbo_flow_protocol_mapper_preflight_request_t *request,
    turbo_flow_protocol_mapper_contract_t *contract);

typedef int (*turbo_flow_protocol_mapper_map_fn)(
    void *ctx, const turbo_flow_protocol_mapper_request_t *request,
    turbo_flow_protocol_mapper_output_t *output);

/**
 * Versioned pre-durable mapper capability.
 *
 * A mapper is selected and retained during generation/session assembly.
 * preflight freezes the exact canonical business descriptor and bounds; the
 * data path calls map directly through this compiled vtable. Neither callback
 * performs registry, symbol, schema-kind, protocol fallback, or
 * alternate-mapper lookup.
 */
typedef struct turbo_flow_protocol_mapper_v1_s {
  size_t size;
  uint32_t abi_version;
  const char *name;
  turbo_flow_protocol_kind_t protocol;
  const char *profile;
  size_t max_semantic_bytes;
  size_t max_output_bytes;
  void *ctx;
  turbo_flow_protocol_mapper_preflight_fn preflight;
  turbo_flow_protocol_mapper_map_fn map;
} turbo_flow_protocol_mapper_v1_t;

#define TURBO_FLOW_PROTOCOL_MAPPER_V1_INIT                                                        \
  {sizeof(turbo_flow_protocol_mapper_v1_t),                                                       \
   TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION,                                                        \
   NULL,                                                                                          \
   TURBO_FLOW_PROTOCOL_MQTT_SN,                                                                   \
   NULL,                                                                                          \
   0u,                                                                                            \
   0u,                                                                                            \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_MAPPER_H */
