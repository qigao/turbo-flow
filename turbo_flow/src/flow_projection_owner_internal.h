#ifndef FLOW_PROJECTION_OWNER_INTERNAL_H
#define FLOW_PROJECTION_OWNER_INTERNAL_H
#include "turbo_flow_projection.h"

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
} flow_msg_projection_t;

int flow_projection_owner_reserve(turbo_flow_projection_owner_t *owner);
void flow_projection_owner_release(turbo_flow_projection_owner_t *owner);
const turbo_flow_projection_owner_config_t *
flow_projection_owner_config(const turbo_flow_projection_owner_t *owner);
#endif
