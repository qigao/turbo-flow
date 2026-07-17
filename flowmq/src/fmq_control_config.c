#include "turbo_flow_fmq_control.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

static int flow_control_config_error(turbo_flow_config_error_t *error, int status,
                                     const char *channel, const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", channel, field);
    else (void)snprintf(error->path, sizeof(error->path), "$.channels.%s", channel ? channel : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_control_config_size(const json_value_t *fields, const char *field, size_t minimum,
                                    size_t maximum, size_t *out) {
  json_value_t *value = turbo_json_object_get(fields, field);
  double number;
  size_t converted;
  if (!value || turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EINVAL;
  number = turbo_json_number(value);
  if (!isfinite(number) || number < (double)minimum || number > (double)maximum)
    return TURBO_ERANGE;
  converted = (size_t)number;
  if ((double)converted != number) return TURBO_EINVAL;
  *out = converted;
  return TURBO_OK;
}

int turbo_flow_fmq_control_config_resolve(const turbo_flow_resolved_config_t *resolved,
                                          const char *channel_name,
                                          turbo_flow_fmq_control_config_t *out,
                                          turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"protocol_version", "target", "dedup_capacity",
                                        "max_request_bytes"};
  turbo_flow_fmq_control_config_t parsed = TURBO_FLOW_FMQ_CONTROL_CONFIG_INIT;
  turbo_json_doc_t *document = NULL;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *version;
  json_value_t *target;
  const char *json;
  const char *target_text;
  size_t json_len = 0u;
  size_t target_len;
  int rc = TURBO_OK;
  if (!resolved || !channel_name || !channel_name[0] || !out || out->size < sizeof(*out) ||
      !error || error->size < sizeof(*error)) {
    return TURBO_EINVAL;
  }
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_len);
  if (!json || turbo_parse_json((const uint8_t *)json, json_len, &document) != TURBO_OK ||
      !document) {
    return flow_control_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                     "invalid resolved configuration snapshot");
  }
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  kind = channel ? turbo_json_object_get(channel, "kind") : NULL;
  fields = channel ? turbo_json_object_get(channel, "config") : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_control_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                   "channel is not resolved");
    goto done;
  }
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "fmq_control") != 0) {
    rc = flow_control_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                   "channel kind must be fmq_control");
    goto done;
  }
  if (!fields || turbo_json_type(fields) != TURBO_JSON_OBJECT) {
    rc = flow_control_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                   "config must be a mapping");
    goto done;
  }
  for (size_t i = 0u; i < turbo_json_object_size(fields); ++i) {
    const char *field = turbo_json_object_key(fields, i);
    int known = 0;
    for (size_t j = 0u; j < sizeof(allowed) / sizeof(allowed[0]); ++j) {
      if (field && strcmp(field, allowed[j]) == 0) {
        known = 1;
        break;
      }
    }
    if (!known) {
      rc = flow_control_config_error(error, TURBO_EINVAL, channel_name, field,
                                     "unknown FMQ control field");
      goto done;
    }
  }
  version = turbo_json_object_get(fields, "protocol_version");
  target = turbo_json_object_get(fields, "target");
  if (!version || turbo_json_type(version) != TURBO_JSON_NUMBER ||
      turbo_json_number(version) != (double)TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION) {
    rc = flow_control_config_error(error, TURBO_ENOTSUP, channel_name, "protocol_version",
                                   "protocol_version must be integer 1");
    goto done;
  }
  if (!target || turbo_json_type(target) != TURBO_JSON_STRING ||
      !(target_text = turbo_json_string(target)) || target_text[0] == '\0') {
    rc = flow_control_config_error(error, TURBO_EINVAL, channel_name, "target",
                                   "target must be a non-empty string");
    goto done;
  }
  target_len = strlen(target_text);
  if (target_len > TURBO_FLOW_FMQ_CONTROL_TARGET_MAX) {
    rc = flow_control_config_error(error, TURBO_ENAMETOOLONG, channel_name, "target",
                                   "target exceeds the Control V1 limit");
    goto done;
  }
  memcpy(parsed.target, target_text, target_len + 1u);
  if (turbo_json_object_get(fields, "dedup_capacity")) {
    rc = flow_control_config_size(fields, "dedup_capacity", 1u, TURBO_FLOW_FMQ_CONTROL_DEDUP_MAX,
                                  &parsed.dedup_capacity);
    if (rc != TURBO_OK) {
      rc = flow_control_config_error(error, rc, channel_name, "dedup_capacity",
                                     "dedup_capacity must be a bounded positive integer");
      goto done;
    }
  }
  if (turbo_json_object_get(fields, "max_request_bytes")) {
    rc = flow_control_config_size(
        fields, "max_request_bytes", TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE,
        TURBO_FLOW_FMQ_CONTROL_REQUEST_MAX_SIZE, &parsed.max_request_bytes);
    if (rc != TURBO_OK) {
      rc = flow_control_config_error(error, rc, channel_name, "max_request_bytes",
                                     "max_request_bytes is outside the Control V1 envelope limit");
      goto done;
    }
  }
  *out = parsed;

done:
  turbo_free_json(&document);
  return rc;
}
