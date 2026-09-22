#ifndef TURBO_FLOW_PROTOCOL_MAPPER_H
#define TURBO_FLOW_PROTOCOL_MAPPER_H

#include "turbo_flow_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION 1u
#define TURBO_FLOW_PROTOCOL_MAPPER_NAME_MAX 63u
#define TURBO_FLOW_PROTOCOL_MAPPER_PROFILE_MAX 63u
#define TURBO_FLOW_PROTOCOL_MAPPER_SCHEMA_ID_MAX 127u
#define TURBO_FLOW_PROTOCOL_MAPPER_TYPE_NAME_MAX 127u
#define TURBO_FLOW_PROTOCOL_MAPPER_MEDIA_TYPE_MAX 63u

typedef enum turbo_flow_protocol_mapper_encoding_e {
  TURBO_FLOW_PROTOCOL_MAPPER_ENCODING_TBE = 1,
  TURBO_FLOW_PROTOCOL_MAPPER_ENCODING_JSON,
  TURBO_FLOW_PROTOCOL_MAPPER_ENCODING_CSV,
  TURBO_FLOW_PROTOCOL_MAPPER_ENCODING_XML,
  TURBO_FLOW_PROTOCOL_MAPPER_ENCODING_UTF8,
  TURBO_FLOW_PROTOCOL_MAPPER_ENCODING_OPAQUE
} turbo_flow_protocol_mapper_encoding_t;

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
  {sizeof(turbo_flow_protocol_mapper_request_t),                                                  \
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
 * A successful mapper call publishes exact schema/type/version/encoding/media
 * identity together with payload_size. On failure payload_size must remain
 * zero and no partial business object is admitted.
 */
typedef struct turbo_flow_protocol_mapper_output_s {
  size_t size;
  uint32_t abi_version;
  uint8_t *payload;
  size_t payload_capacity;
  size_t payload_size;
  char schema_id[TURBO_FLOW_PROTOCOL_MAPPER_SCHEMA_ID_MAX + 1u];
  char type_name[TURBO_FLOW_PROTOCOL_MAPPER_TYPE_NAME_MAX + 1u];
  uint32_t schema_version;
  turbo_flow_protocol_mapper_encoding_t encoding;
  char media_type[TURBO_FLOW_PROTOCOL_MAPPER_MEDIA_TYPE_MAX + 1u];
} turbo_flow_protocol_mapper_output_t;

#define TURBO_FLOW_PROTOCOL_MAPPER_OUTPUT_INIT                                                    \
  {sizeof(turbo_flow_protocol_mapper_output_t),                                                   \
   TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION,                                                        \
   NULL,                                                                                          \
   0u,                                                                                            \
   0u,                                                                                            \
   {0},                                                                                           \
   {0},                                                                                           \
   0u,                                                                                            \
   0,                                                                                             \
   {0}}

typedef int (*turbo_flow_protocol_mapper_map_fn)(
    void *ctx, const turbo_flow_protocol_mapper_request_t *request,
    turbo_flow_protocol_mapper_output_t *output);

/**
 * Versioned pre-durable mapper capability.
 *
 * A mapper is selected and retained during generation/session assembly. The
 * data path calls this compiled vtable directly; it performs no registry,
 * symbol, schema-kind, protocol fallback, or alternate-mapper lookup.
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
   NULL}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROTOCOL_MAPPER_H */
