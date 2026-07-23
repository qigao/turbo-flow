#include "flow_internal.h"

#include <stdlib.h>
#include <string.h>

#define FLOW_MSG_PROJECTION_MAGIC UINT64_C(0x544650524f4a5631)

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
  turbo_flow_protocol_route_t route;
  int has_route;
  turbo_flow_protocol_origin_t protocol_origin;
  int has_protocol_origin;
  turbo_flow_protocol_settlement_envelope_t protocol_settlement;
  int has_protocol_settlement;
} flow_msg_projection_t;

static int flow_msg_projection_empty(const flow_msg_projection_t *projection) {
  return projection && !projection->descriptor && !projection->value && !projection->has_route &&
         !projection->has_protocol_origin && !projection->has_protocol_settlement;
}

static void flow_msg_projection_destroy(void *ptr, void *ctx) {
  flow_msg_projection_t *projection = (flow_msg_projection_t *)ptr;

  (void)ctx;
  if (!projection || projection->magic != FLOW_MSG_PROJECTION_MAGIC) return;
  projection->magic = 0u;
  if (projection->value && projection->destroy) {
    projection->destroy(projection->value, projection->ctx);
  }
  free(projection);
}

static const flow_msg_projection_t *flow_msg_projection(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *projection;

  if (!msg || !msg->_content_handle) return NULL;
  projection = (const flow_msg_projection_t *)msg->_content_handle;
  return projection->magic == FLOW_MSG_PROJECTION_MAGIC ? projection : NULL;
}

static int flow_msg_descriptor_accepts_schema(const turbo_flow_content_descriptor_t *descriptor,
                                              const turbo_flow_data_schema_t *schema) {
  if (!descriptor || !schema || (descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u) {
    return TURBO_OK;
  }
  return descriptor->encoding == schema->encoding &&
                 descriptor->schema_version == schema->schema_version &&
                 strcmp(descriptor->schema_name, schema->schema_name) == 0 &&
                 strcmp(descriptor->type_name, schema->type_name) == 0
             ? TURBO_OK
             : TURBO_EPROTO;
}

static int flow_msg_projection_clone(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src) {
  const flow_msg_projection_t *source = flow_msg_projection(src);
  flow_msg_projection_t *copy;
  void *value = NULL;
  int rc;

  if (!source) return TURBO_EINVAL;
  if (source->value) {
    if (!source->clone) return TURBO_ENOTSUP;
    rc = source->clone(source->value, source->ctx, &value);
    if (rc != TURBO_OK) return rc;
    if (!value) return TURBO_EPROTO;
  }
  copy = (flow_msg_projection_t *)calloc(1, sizeof(*copy));
  if (!copy) {
    if (value) source->destroy(value, source->ctx);
    return TURBO_ENOMEM;
  }
  *copy = *source;
  if (copy->owns_descriptor) copy->descriptor = &copy->owned_descriptor;
  copy->value = value;
  dst->_content_handle = copy;
  return TURBO_OK;
}

static void flow_msg_failure_cleanup(turbo_flow_failure_t *failure) {
  if (!failure) return;
  tstr_freep(&failure->stage_name);
  tstr_freep(&failure->adapter_name);
  tstr_freep(&failure->route_name);
  failure->code = TURBO_OK;
  failure->attempt = 0;
}

static int flow_msg_failure_copy(turbo_flow_failure_t *dst, const turbo_flow_failure_t *src) {
  if (!dst || !src) return TURBO_EINVAL;
  memset(dst, 0, sizeof(*dst));
  if (src->stage_name && !(dst->stage_name = tstr_from_v(tstr_to_v(src->stage_name)))) goto nomem;
  if (src->adapter_name && !(dst->adapter_name = tstr_from_v(tstr_to_v(src->adapter_name)))) {
    goto nomem;
  }
  if (src->route_name && !(dst->route_name = tstr_from_v(tstr_to_v(src->route_name)))) goto nomem;
  dst->code = src->code;
  dst->attempt = src->attempt;
  return TURBO_OK;

nomem:
  flow_msg_failure_cleanup(dst);
  return TURBO_ENOMEM;
}

int flow_msg_transport_context_is_borrowed(const turbo_flow_msg_t *msg) {
  uintptr_t context_address;
  uintptr_t buffer_address;
  size_t offset;
  size_t used;

  if (!msg || !msg->transport_context) return 0;
  if (!msg->buffer) return 1;
  used = mem_buffer_used(msg->buffer);
  if (used == 0u) return 1;
  context_address = (uintptr_t)msg->transport_context;
  buffer_address = (uintptr_t)mem_buffer_const_data(msg->buffer);
  if (context_address < buffer_address) return 1;
  offset = (size_t)(context_address - buffer_address);
  return offset >= used;
}

int flow_msg_set_failure(turbo_flow_msg_t *msg, const char *stage_name, const char *adapter_name,
                         const char *route_name, int code, uint32_t attempt) {
  turbo_flow_failure_t failure;

  if (!msg || !stage_name || !route_name || code == TURBO_OK || attempt == 0u) {
    return TURBO_EINVAL;
  }
  memset(&failure, 0, sizeof(failure));
  failure.stage_name = tstr_dup(stage_name);
  failure.route_name = tstr_dup(route_name);
  if (adapter_name) failure.adapter_name = tstr_dup(adapter_name);
  if (!failure.stage_name || !failure.route_name || (adapter_name && !failure.adapter_name)) {
    flow_msg_failure_cleanup(&failure);
    return TURBO_ENOMEM;
  }
  failure.code = code;
  failure.attempt = attempt;
  flow_msg_failure_cleanup(&msg->failure);
  msg->failure = failure;
  return TURBO_OK;
}

void turbo_flow_msg_init(turbo_flow_msg_t *msg) {
  if (!msg) return;
  memset(msg, 0, sizeof(*msg));
  msg->data_decision = (turbo_flow_data_decision_t)TURBO_FLOW_DATA_DECISION_INIT;
}

void turbo_flow_msg_cleanup(turbo_flow_msg_t *msg) {
  if (!msg) return;
  flow_msg_projection_destroy(msg->_content_handle, NULL);
  flow_msg_failure_cleanup(&msg->failure);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  turbo_flow_msg_init(msg);
}

int turbo_flow_msg_retain_view(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src) {
  const flow_msg_projection_t *source_binding;
  flow_msg_projection_t *binding_copy = NULL;

  if (!dst || !src) return TURBO_EINVAL;
  source_binding = flow_msg_projection(src);
  if (src->owned_payload || (source_binding && source_binding->value)) {
    return TURBO_EINVAL;
  }
  if (source_binding) {
    binding_copy = (flow_msg_projection_t *)calloc(1, sizeof(*binding_copy));
    if (!binding_copy) return TURBO_ENOMEM;
    *binding_copy = *source_binding;
    if (binding_copy->owns_descriptor) binding_copy->descriptor = &binding_copy->owned_descriptor;
  }

  turbo_flow_msg_init(dst);
  *dst = *src;
  dst->buffer = mem_buffer_retain(src->buffer);
  dst->owned_payload = NULL;
  dst->_content_handle = binding_copy;
  memset(&dst->failure, 0, sizeof(dst->failure));
  if (flow_msg_failure_copy(&dst->failure, &src->failure) != TURBO_OK) {
    mem_buffer_release(dst->buffer);
    free(binding_copy);
    turbo_flow_msg_init(dst);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

int turbo_flow_msg_clone(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src) {
  int has_projection;

  if (!dst || !src) return TURBO_EINVAL;
  has_projection = flow_msg_projection(src) != NULL;
  turbo_flow_msg_init(dst);
  *dst = *src;
  dst->buffer = mem_buffer_retain(src->buffer);
  dst->owned_payload = NULL;
  dst->_content_handle = NULL;
  memset(&dst->failure, 0, sizeof(dst->failure));
  if (flow_msg_failure_copy(&dst->failure, &src->failure) != TURBO_OK) {
    mem_buffer_release(dst->buffer);
    turbo_flow_msg_init(dst);
    return TURBO_ENOMEM;
  }

  if (src->owned_payload) {
    uintptr_t payload_addr = (uintptr_t)src->payload.data;
    uintptr_t owned_start = (uintptr_t)src->owned_payload;
    uintptr_t owned_end = owned_start + tstr_len(src->owned_payload);

    dst->owned_payload = tstr_from_v(tstr_to_v(src->owned_payload));
    if (!dst->owned_payload) {
      mem_buffer_release(dst->buffer);
      turbo_flow_msg_init(dst);
      return TURBO_ENOMEM;
    }
    if (payload_addr >= owned_start && payload_addr <= owned_end) {
      size_t offset = (size_t)(payload_addr - owned_start);
      size_t owned_len = tstr_len(dst->owned_payload);
      if (offset > owned_len || src->payload.len > owned_len - offset) {
        turbo_flow_msg_cleanup(dst);
        return TURBO_EINVAL;
      }
      dst->payload = tstr_v_from_buf(dst->owned_payload + offset, src->payload.len);
    } else {
      dst->payload = tstr_to_v(dst->owned_payload);
    }
  }

  if (has_projection) {
    int rc = flow_msg_projection_clone(dst, src);
    if (rc != TURBO_OK) {
      turbo_flow_msg_cleanup(dst);
      return rc;
    }
  }

  return TURBO_OK;
}

int turbo_flow_msg_move(turbo_flow_msg_t *dst, turbo_flow_msg_t *src) {
  if (!dst || !src) return TURBO_EINVAL;
  turbo_flow_msg_init(dst);
  *dst = *src;
  turbo_flow_msg_init(src);
  return TURBO_OK;
}

turbo_flow_content_state_t turbo_flow_msg_content_state(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *projection = flow_msg_projection(msg);
  return projection && projection->value ? TURBO_FLOW_CONTENT_SCHEMA_BOUND
                                         : TURBO_FLOW_CONTENT_OPAQUE;
}

int turbo_flow_msg_bind_projection(turbo_flow_msg_t *msg, const turbo_flow_data_schema_t *schema,
                                   void *projection, turbo_flow_projection_clone_fn clone,
                                   turbo_flow_destroy_fn destroy, void *ctx) {
  flow_msg_projection_t *binding;

  if (!msg || !schema || schema->size < sizeof(*schema) ||
      schema->domain == TURBO_FLOW_DOMAIN_NONE || !schema->schema_name ||
      schema->schema_name[0] == '\0' || !schema->type_name || schema->type_name[0] == '\0' ||
      !schema->projection_type || schema->projection_type[0] == '\0' ||
      schema->schema_version == 0u || schema->encoding < TURBO_FLOW_DATA_ENCODING_TBE ||
      schema->encoding > TURBO_FLOW_DATA_ENCODING_OPAQUE || !projection || !destroy) {
    return TURBO_EINVAL;
  }
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (binding && binding->value) return TURBO_EBUSY;
  if (binding && flow_msg_descriptor_accepts_schema(binding->descriptor, schema) != TURBO_OK) {
    return TURBO_EPROTO;
  }
  if (!binding) {
    binding = (flow_msg_projection_t *)calloc(1, sizeof(*binding));
    if (!binding) return TURBO_ENOMEM;
    binding->magic = FLOW_MSG_PROJECTION_MAGIC;
  }
  binding->schema = schema;
  binding->value = projection;
  binding->clone = clone;
  binding->destroy = destroy;
  binding->ctx = ctx;
  msg->_content_handle = binding;
  return TURBO_OK;
}

const void *turbo_flow_msg_projection(const turbo_flow_msg_t *msg,
                                      const turbo_flow_data_schema_t **schema_out) {
  const flow_msg_projection_t *projection = flow_msg_projection(msg);

  if (schema_out) *schema_out = projection && projection->value ? projection->schema : NULL;
  return projection ? projection->value : NULL;
}

void turbo_flow_msg_clear_projection(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *projection = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!projection || !projection->value) return;
  projection->destroy(projection->value, projection->ctx);
  projection->value = NULL;
  projection->schema = NULL;
  projection->clone = NULL;
  projection->destroy = NULL;
  projection->ctx = NULL;
  if (flow_msg_projection_empty(projection)) {
    free(projection);
    msg->_content_handle = NULL;
  }
}

void turbo_flow_msg_clear_content(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *projection;
  if (!msg) return;
  projection = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!projection) return;
  if (projection->value && projection->destroy) {
    projection->destroy(projection->value, projection->ctx);
  }
  projection->descriptor = NULL;
  projection->owns_descriptor = 0;
  memset(&projection->owned_descriptor, 0, sizeof(projection->owned_descriptor));
  projection->schema = NULL;
  projection->value = NULL;
  projection->clone = NULL;
  projection->destroy = NULL;
  projection->ctx = NULL;
  if (flow_msg_projection_empty(projection)) {
    free(projection);
    msg->_content_handle = NULL;
  }
}

int turbo_flow_msg_set_content_descriptor(turbo_flow_msg_t *msg,
                                          const turbo_flow_content_descriptor_t *descriptor) {
  flow_msg_projection_t *binding;
  if (!msg || turbo_flow_content_descriptor_check(descriptor) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (binding && binding->schema &&
      flow_msg_descriptor_accepts_schema(descriptor, binding->schema) != TURBO_OK) {
    return TURBO_EPROTO;
  }
  if (binding && binding->descriptor &&
      turbo_flow_content_descriptor_validate(binding->descriptor, descriptor) != TURBO_OK) {
    return TURBO_EPROTO;
  }
  if (!binding) {
    binding = (flow_msg_projection_t *)calloc(1, sizeof(*binding));
    if (!binding) return TURBO_ENOMEM;
    binding->magic = FLOW_MSG_PROJECTION_MAGIC;
    msg->_content_handle = binding;
  }
  binding->descriptor = descriptor;
  binding->owns_descriptor = 0;
  return TURBO_OK;
}

int turbo_flow_msg_copy_content_descriptor(turbo_flow_msg_t *msg,
                                           const turbo_flow_content_descriptor_t *descriptor) {
  flow_msg_projection_t *binding;
  int rc = turbo_flow_msg_set_content_descriptor(msg, descriptor);
  if (rc != TURBO_OK) return rc;
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  binding->owned_descriptor = *descriptor;
  binding->owned_descriptor.size = sizeof(binding->owned_descriptor);
  binding->descriptor = &binding->owned_descriptor;
  binding->owns_descriptor = 1;
  return TURBO_OK;
}

const turbo_flow_content_descriptor_t *
turbo_flow_msg_content_descriptor(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding ? binding->descriptor : NULL;
}

int turbo_flow_msg_content_descriptor_owned(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding && binding->descriptor && binding->owns_descriptor;
}

int turbo_flow_msg_set_protocol_route(turbo_flow_msg_t *msg,
                                      const turbo_flow_protocol_route_t *route) {
  flow_msg_projection_t *binding;
  if (!msg || !route || route->size < sizeof(*route) ||
      route->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION ||
      route->protocol < TURBO_FLOW_PROTOCOL_FMQ || route->protocol > TURBO_FLOW_PROTOCOL_MQTT ||
      route->reserved != 0u || route->owner_instance_id == 0u || route->session_id == 0u ||
      route->session_generation == 0u) {
    return TURBO_EINVAL;
  }
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (binding && binding->has_protocol_settlement &&
      (binding->protocol_settlement.message.protocol != route->protocol ||
       binding->protocol_settlement.message.session_generation != route->session_generation))
    return TURBO_EPROTO;
  if (!binding) {
    binding = (flow_msg_projection_t *)calloc(1, sizeof(*binding));
    if (!binding) return TURBO_ENOMEM;
    binding->magic = FLOW_MSG_PROJECTION_MAGIC;
    msg->_content_handle = binding;
  }
  binding->route = *route;
  binding->route.size = sizeof(binding->route);
  binding->has_route = 1;
  return TURBO_OK;
}

const turbo_flow_protocol_route_t *turbo_flow_msg_protocol_route(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding && binding->has_route ? &binding->route : NULL;
}

void turbo_flow_msg_clear_protocol_route(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!binding || !binding->has_route) return;
  memset(&binding->protocol_settlement, 0, sizeof(binding->protocol_settlement));
  binding->has_protocol_settlement = 0;
  memset(&binding->route, 0, sizeof(binding->route));
  binding->has_route = 0;
  if (flow_msg_projection_empty(binding)) {
    free(binding);
    msg->_content_handle = NULL;
  }
}

int turbo_flow_msg_set_protocol_origin(turbo_flow_msg_t *msg,
                                       const turbo_flow_protocol_origin_t *origin) {
  flow_msg_projection_t *binding;
  if (!msg || turbo_flow_protocol_origin_validate(origin) != TURBO_OK) return TURBO_EINVAL;
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (binding && binding->has_protocol_origin) return TURBO_EALREADY;
  if (!binding) {
    binding = (flow_msg_projection_t *)calloc(1, sizeof(*binding));
    if (!binding) return TURBO_ENOMEM;
    binding->magic = FLOW_MSG_PROJECTION_MAGIC;
    msg->_content_handle = binding;
  }
  binding->protocol_origin = *origin;
  binding->protocol_origin.size = sizeof(binding->protocol_origin);
  binding->has_protocol_origin = 1;
  return TURBO_OK;
}

const turbo_flow_protocol_origin_t *turbo_flow_msg_protocol_origin(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding && binding->has_protocol_origin ? &binding->protocol_origin : NULL;
}

void turbo_flow_msg_clear_protocol_origin(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!binding || !binding->has_protocol_origin) return;
  memset(&binding->protocol_origin, 0, sizeof(binding->protocol_origin));
  binding->has_protocol_origin = 0;
  if (flow_msg_projection_empty(binding)) {
    free(binding);
    msg->_content_handle = NULL;
  }
}

int turbo_flow_msg_set_protocol_settlement(
    turbo_flow_msg_t *msg, const turbo_flow_protocol_settlement_envelope_t *envelope) {
  flow_msg_projection_t *binding;
  const turbo_flow_protocol_route_t *route;
  if (!msg || !envelope || envelope->size < sizeof(*envelope) ||
      envelope->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION ||
      turbo_flow_protocol_message_validate(&envelope->message) != TURBO_OK ||
      envelope->requested_point < TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED ||
      envelope->requested_point > TURBO_FLOW_PROTOCOL_SETTLE_DURABLE ||
      envelope->settled_point != 0) {
    return TURBO_EINVAL;
  }
  binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  route = binding && binding->has_route ? &binding->route : NULL;
  if (!route || route->protocol != envelope->message.protocol ||
      route->session_generation != envelope->message.session_generation)
    return TURBO_EPROTO;
  if (binding->has_protocol_settlement) return TURBO_EALREADY;
  binding->protocol_settlement = *envelope;
  binding->protocol_settlement.size = sizeof(binding->protocol_settlement);
  binding->has_protocol_settlement = 1;
  return TURBO_OK;
}

const turbo_flow_protocol_settlement_envelope_t *
turbo_flow_msg_protocol_settlement(const turbo_flow_msg_t *msg) {
  const flow_msg_projection_t *binding = flow_msg_projection(msg);
  return binding && binding->has_protocol_settlement ? &binding->protocol_settlement : NULL;
}

int turbo_flow_msg_complete_protocol_settlement(turbo_flow_msg_t *msg,
                                                turbo_flow_protocol_settlement_point_t point) {
  flow_msg_projection_t *binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!binding || !binding->has_protocol_settlement || point == 0 ||
      point != binding->protocol_settlement.requested_point)
    return TURBO_EINVAL;
  if (binding->protocol_settlement.settled_point != 0) return TURBO_EALREADY;
  binding->protocol_settlement.settled_point = point;
  return TURBO_OK;
}

void turbo_flow_msg_clear_protocol_settlement(turbo_flow_msg_t *msg) {
  flow_msg_projection_t *binding = (flow_msg_projection_t *)flow_msg_projection(msg);
  if (!binding || !binding->has_protocol_settlement) return;
  memset(&binding->protocol_settlement, 0, sizeof(binding->protocol_settlement));
  binding->has_protocol_settlement = 0;
  if (flow_msg_projection_empty(binding)) {
    free(binding);
    msg->_content_handle = NULL;
  }
}
