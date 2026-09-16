#include "turbo_flow_protocol_business_plugin.h"

#include "salts_error.h"

#include <stdlib.h>
#include <string.h>

struct turbo_flow_protocol_business_s {
  turbo_flow_protocol_business_info_t info;
  turbo_flow_protocol_business_ops_t ops;
  void *ctx;
};

static size_t flow_protocol_business_bounded_length(const char *text, size_t capacity) {
  size_t size = 0u;
  if (!text) return 0u;
  while (size < capacity && text[size] != '\0')
    size++;
  return size;
}

static int flow_protocol_business_text_valid(const char *text, size_t max_size, int optional) {
  size_t size;
  if (!text) return optional ? SALTS_OK : SALTS_EINVAL;
  size = flow_protocol_business_bounded_length(text, max_size + 1u);
  if (size == 0u) return SALTS_EINVAL;
  if (size > max_size) return SALTS_EMSGSIZE;
  for (size_t i = 0u; i < size; ++i) {
    const unsigned char ch = (unsigned char)text[i];
    if (ch < 0x20u || ch == 0x7fu) return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int flow_protocol_business_protocol_valid(turbo_flow_protocol_kind_t protocol) {
  return protocol >= TURBO_FLOW_PROTOCOL_MQTT_SN &&
         protocol <= TURBO_FLOW_PROTOCOL_JTT_808;
}

static int flow_protocol_business_segment_valid(const char *text, size_t max_size) {
  size_t size;
  if (flow_protocol_business_text_valid(text, max_size, 0) != SALTS_OK) return SALTS_EINVAL;
  size = flow_protocol_business_bounded_length(text, max_size + 1u);
  for (size_t i = 0u; i < size; ++i) {
    const unsigned char ch = (unsigned char)text[i];
    if (ch <= 0x20u || ch >= 0x7fu || ch == '/' || ch == '+' || ch == '#') return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int
flow_protocol_business_content_validate(const turbo_flow_protocol_business_content_view_t *content,
                                       size_t max_payload_size) {
  int rc;
  if (!content || content->size < sizeof(*content) ||
      content->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      (!content->data && content->data_size != 0u) || content->data_size > max_payload_size)
    return content && content->data_size > max_payload_size ? SALTS_EMSGSIZE : SALTS_EINVAL;
  rc = flow_protocol_business_text_valid(content->media_type,
                                        TURBO_FLOW_PROTOCOL_BUSINESS_MEDIA_TYPE_MAX, 0);
  if (rc != SALTS_OK) return rc;
  if ((content->schema_id == NULL) != (content->type_name == NULL)) return SALTS_EINVAL;
  rc = flow_protocol_business_text_valid(content->schema_id,
                                        TURBO_FLOW_PROTOCOL_BUSINESS_SCHEMA_ID_MAX, 1);
  if (rc != SALTS_OK) return rc;
  return flow_protocol_business_text_valid(content->type_name,
                                          TURBO_FLOW_PROTOCOL_BUSINESS_TYPE_NAME_MAX, 1);
}

static int
flow_protocol_business_metadata_validate(const turbo_flow_protocol_metadata_t *metadata,
                                        turbo_flow_protocol_kind_t expected_protocol) {
  if (!metadata || metadata->size < sizeof(*metadata) ||
      metadata->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      metadata->protocol != expected_protocol ||
      metadata->direction != TURBO_FLOW_PROTOCOL_DIRECTION_UP ||
      flow_protocol_business_segment_valid(metadata->device_id, TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX) !=
          SALTS_OK ||
      flow_protocol_business_segment_valid(metadata->operation, TURBO_FLOW_PROTOCOL_OPERATION_MAX) !=
          SALTS_OK)
    return SALTS_EINVAL;
  return SALTS_OK;
}

int turbo_flow_protocol_business_create(const char *business_name,
                                       turbo_flow_protocol_kind_t protocol, const char *profile,
                                       size_t max_payload_size,
                                       turbo_flow_protocol_business_capabilities_t capabilities,
                                       const turbo_flow_protocol_business_ops_t *ops, void *ctx,
                                       turbo_flow_protocol_business_t **out) {
  turbo_flow_protocol_business_t *business;
  int rc;
  if (out) *out = NULL;
  rc = flow_protocol_business_text_valid(business_name, TURBO_FLOW_PROTOCOL_BUSINESS_NAME_MAX, 0);
  if (rc != SALTS_OK || !flow_protocol_business_protocol_valid(protocol) || !profile ||
      max_payload_size == 0u || !ops || ops->size < sizeof(*ops) ||
      ops->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION || !out)
    return rc != SALTS_OK ? rc : SALTS_EINVAL;
  rc = flow_protocol_business_text_valid(profile, TURBO_FLOW_PROTOCOL_BUSINESS_PROFILE_MAX, 0);
  if (rc != SALTS_OK) return rc;
  if ((capabilities & TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT) != 0u &&
      !ops->consume_committed)
    return SALTS_EINVAL;
  if ((capabilities & TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND) != 0u &&
      !ops->prepare_command)
    return SALTS_EINVAL;
  if ((capabilities & ~(TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT |
                        TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND)) != 0u ||
      capabilities == 0u)
    return SALTS_EINVAL;
  business = (turbo_flow_protocol_business_t *)calloc(1u, sizeof(*business));
  if (!business) return SALTS_ENOMEM;
  business->info = (turbo_flow_protocol_business_info_t)TURBO_FLOW_PROTOCOL_BUSINESS_INFO_INIT;
  business->info.protocol = protocol;
  business->info.capabilities = capabilities;
  business->info.max_payload_size = max_payload_size;
  memcpy(business->info.business, business_name, strlen(business_name) + 1u);
  memcpy(business->info.profile, profile, strlen(profile) + 1u);
  business->ops = *ops;
  business->ctx = ctx;
  *out = business;
  return SALTS_OK;
}

void turbo_flow_protocol_business_destroy(turbo_flow_protocol_business_t *business) {
  free(business);
}

int turbo_flow_protocol_business_consume_committed(
    turbo_flow_protocol_business_t *business,
    const turbo_flow_protocol_business_event_view_t *event) {
  int rc;
  if (!business || !event || event->size < sizeof(*event) ||
      event->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION || event->delivery_id == 0u ||
      !event->route || event->route_size == 0u ||
      event->route_size > TURBO_FLOW_PROTOCOL_BUSINESS_ROUTE_MAX)
    return SALTS_EINVAL;
  if ((business->info.capabilities & TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT) == 0u ||
      !business->ops.consume_committed)
    return SALTS_ENOTSUP;
  rc = flow_protocol_business_metadata_validate(&event->metadata, business->info.protocol);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_business_content_validate(&event->content, business->info.max_payload_size);
  if (rc != SALTS_OK) return rc;
  return business->ops.consume_committed(business->ctx, event);
}

int turbo_flow_protocol_business_prepare_command(
    turbo_flow_protocol_business_t *business,
    const turbo_flow_protocol_business_command_request_t *request,
    turbo_flow_protocol_business_command_output_t *output) {
  int rc;
  if (!business || !request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      request->command_id == 0u || request->protocol != business->info.protocol || !output ||
      output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      (!output->payload && output->payload_capacity != 0u))
    return SALTS_EINVAL;
  if ((business->info.capabilities & TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND) == 0u ||
      !business->ops.prepare_command)
    return SALTS_ENOTSUP;
  rc = flow_protocol_business_segment_valid(request->tenant, TURBO_FLOW_PROTOCOL_TENANT_MAX);
  if (rc == SALTS_OK)
    rc = flow_protocol_business_segment_valid(request->device_id, TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX);
  if (rc == SALTS_OK)
    rc = flow_protocol_business_text_valid(request->action, TURBO_FLOW_PROTOCOL_OPERATION_MAX, 0);
  if (rc == SALTS_OK)
    rc = flow_protocol_business_text_valid(request->resource, TURBO_FLOW_PROTOCOL_RESOURCE_MAX, 1);
  if (rc == SALTS_OK)
    rc = flow_protocol_business_text_valid(request->correlation_id,
                                          TURBO_FLOW_PROTOCOL_CORRELATION_MAX, 1);
  if (rc == SALTS_OK)
    rc = flow_protocol_business_content_validate(&request->content, business->info.max_payload_size);
  if (rc != SALTS_OK) return rc;
  memset(output->device_id, 0, sizeof(output->device_id));
  memset(output->operation, 0, sizeof(output->operation));
  memset(output->resource, 0, sizeof(output->resource));
  memset(output->correlation_id, 0, sizeof(output->correlation_id));
  output->sequence = 0u;
  output->payload_size = 0u;
  rc = business->ops.prepare_command(business->ctx, request, output);
  if (rc != SALTS_OK) {
    output->payload_size = 0u;
    return rc;
  }
  if (output->payload_size > output->payload_capacity ||
      output->payload_size > business->info.max_payload_size) {
    output->payload_size = 0u;
    return SALTS_EMSGSIZE;
  }
  if (flow_protocol_business_bounded_length(output->device_id, sizeof(output->device_id)) >=
          sizeof(output->device_id) ||
      flow_protocol_business_bounded_length(output->operation, sizeof(output->operation)) >=
          sizeof(output->operation) ||
      flow_protocol_business_bounded_length(output->resource, sizeof(output->resource)) >=
          sizeof(output->resource) ||
      flow_protocol_business_bounded_length(output->correlation_id,
                                           sizeof(output->correlation_id)) >=
          sizeof(output->correlation_id) ||
      output->device_id[0] == '\0' || output->operation[0] == '\0') {
    output->payload_size = 0u;
    return SALTS_EPROTO;
  }
  if (flow_protocol_business_segment_valid(output->device_id, TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX) !=
          SALTS_OK ||
      flow_protocol_business_segment_valid(output->operation, TURBO_FLOW_PROTOCOL_OPERATION_MAX) !=
          SALTS_OK ||
      (output->correlation_id[0] != '\0' &&
       flow_protocol_business_segment_valid(output->correlation_id,
                                           TURBO_FLOW_PROTOCOL_CORRELATION_MAX) != SALTS_OK)) {
    output->payload_size = 0u;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_protocol_business_command_view(
    const turbo_flow_protocol_business_command_output_t *output,
    turbo_flow_protocol_command_view_t *view) {
  if (!output || output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      output->device_id[0] == '\0' || output->operation[0] == '\0' ||
      (!output->payload && output->payload_size != 0u) ||
      output->payload_size > output->payload_capacity || !view || view->size < sizeof(*view) ||
      view->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION)
    return SALTS_EINVAL;
  view->device_id = output->device_id;
  view->operation = output->operation;
  view->resource = output->resource[0] != '\0' ? output->resource : NULL;
  view->correlation_id = output->correlation_id[0] != '\0' ? output->correlation_id : NULL;
  view->sequence = output->sequence;
  view->payload = output->payload;
  view->payload_size = output->payload_size;
  return SALTS_OK;
}

int turbo_flow_protocol_business_get_info(const turbo_flow_protocol_business_t *business,
                                         turbo_flow_protocol_business_info_t *out) {
  if (!business || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION)
    return SALTS_EINVAL;
  *out = business->info;
  return SALTS_OK;
}
