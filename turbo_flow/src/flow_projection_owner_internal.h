#ifndef FLOW_PROJECTION_OWNER_INTERNAL_H
#define FLOW_PROJECTION_OWNER_INTERNAL_H
#include "turbo_flow_projection.h"
#include "turbo_flow_durable_buffer.h"

typedef struct flow_msg_projection_s {
  uint64_t magic;
  const turbo_flow_content_descriptor_t *descriptor;
  const turbo_flow_data_schema_t *schema;
  void *value;
  turbo_flow_projection_clone_fn clone;
  turbo_flow_destroy_fn destroy;
  void *ctx;
  turbo_flow_content_descriptor_t owned_descriptor;
  int owns_descriptor;
  turbo_flow_projection_owner_t *owner;
  const cmeta_data_desc *data;
  const turbo_flow_data_schema_t *result_schema;
  const cmeta_data_desc *result_data;
  void *result_value;
  turbo_flow_projection_clone_fn result_clone;
  turbo_flow_destroy_fn result_destroy;
  void *result_ctx;
  turbo_flow_projection_owner_t *result_owner;
  int claim_active;
  int has_durable_identity;
  int has_durable_claim;
  size_t durable_source_id_len;
  size_t durable_device_id_len;
  size_t durable_session_id_len;
  size_t durable_partition_key_len;
  size_t durable_admission_id_len;
  size_t durable_correlation_len;
  uint64_t durable_source_sequence;
  char durable_source_id[TURBO_FLOW_DURABLE_SOURCE_ID_MAX + 1u];
  char durable_device_id[TURBO_FLOW_DURABLE_PARTITION_COMPONENT_MAX + 1u];
  char durable_session_id[TURBO_FLOW_DURABLE_PARTITION_COMPONENT_MAX + 1u];
  char durable_partition_key[TURBO_FLOW_DURABLE_PARTITION_COMPONENT_MAX + 1u];
  char durable_admission_id[TURBO_FLOW_DURABLE_ADMISSION_ID_MAX + 1u];
  char durable_correlation[TURBO_FLOW_DURABLE_CORRELATION_MAX + 1u];
} flow_msg_projection_t;

struct turbo_flow_result_claim_s {
  turbo_flow_msg_t *msg;
  flow_msg_projection_t *original;
  int owns_original;
  flow_msg_projection_t *prepared;
  turbo_flow_projection_owner_t *owner;
};

int flow_msg_mark_durable_claim(turbo_flow_msg_t *msg);
int flow_msg_has_durable_claim(const turbo_flow_msg_t *msg);
int flow_projection_owner_reserve(turbo_flow_projection_owner_t *owner);
void flow_projection_owner_release(turbo_flow_projection_owner_t *owner);
const turbo_flow_projection_owner_config_t *
flow_projection_owner_config(const turbo_flow_projection_owner_t *owner);
#endif
