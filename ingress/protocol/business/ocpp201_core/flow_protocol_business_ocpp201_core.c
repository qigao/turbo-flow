#include "flow_protocol_plugin_support.h"
#include "ocpp201_core.h"
#include "salts_error.h"
#include "turbo_flow_protocol_business_plugin.h"
#include <json_parser.h>

#include <stdlib.h>
#include <string.h>

#define FLOW_OCPP201_CORE_BUSINESS "ocpp201-core"
#define FLOW_OCPP201_CORE_PROFILE "ocpp-2.0.1-core-minimal"
#define FLOW_OCPP201_CORE_VERSION "2.0.1"
#define FLOW_OCPP201_CORE_SCHEMA_ID "Ocpp201Core"
#define FLOW_OCPP201_CORE_JSON_MEDIA_TYPE "application/json"
#define FLOW_OCPP201_CORE_REASON_JSON_MAX 96u

typedef struct flow_ocpp201_core_owner_s {
  DataBind *codec;
  turbo_flow_protocol_business_t *business;
} flow_ocpp201_core_owner_t;

static int flow_ocpp201_core_databind_status(DataBindStatus status) {
  switch (status) {
  case DATA_BIND_OK:
    return SALTS_OK;
  case DATA_BIND_ERR_INVALID_ARG:
    return SALTS_EINVAL;
  case DATA_BIND_ERR_IO:
    return SALTS_EIO;
  case DATA_BIND_ERR_TYPE_NOT_FOUND:
    return SALTS_ENOENT;
  case DATA_BIND_ERR_OOM:
    return SALTS_ENOMEM;
  case DATA_BIND_ERR_PARSE:
  case DATA_BIND_ERR_SCHEMA:
  case DATA_BIND_ERR_TYPE_MISMATCH:
  case DATA_BIND_ERR_RUNTIME:
  default:
    return SALTS_EPROTO;
  }
}

static int
flow_ocpp201_core_content_is_raw_json(const turbo_flow_protocol_business_content_view_t *content) {
  if (!content || strcmp(content->media_type, FLOW_OCPP201_CORE_JSON_MEDIA_TYPE) != 0)
    return SALTS_EINVAL;
  if (content->schema_id || content->type_name) return SALTS_EINVAL;
  return SALTS_OK;
}

static int
flow_ocpp201_core_content_is_typed_json(const turbo_flow_protocol_business_content_view_t *content,
                                        const char *type_name) {
  if (!content || !type_name ||
      strcmp(content->media_type, FLOW_OCPP201_CORE_JSON_MEDIA_TYPE) != 0 || !content->schema_id ||
      !content->type_name || strcmp(content->schema_id, FLOW_OCPP201_CORE_SCHEMA_ID) != 0 ||
      strcmp(content->type_name, type_name) != 0)
    return SALTS_EINVAL;
  return SALTS_OK;
}

static int flow_ocpp201_core_ascii_digit(char value) { return value >= '0' && value <= '9'; }

static unsigned flow_ocpp201_core_decimal(const char *text, size_t size) {
  unsigned value = 0u;
  size_t index;

  for (index = 0u; index < size; ++index)
    value = value * 10u + (unsigned)(text[index] - '0');
  return value;
}

static unsigned flow_ocpp201_core_days_in_month(unsigned year, unsigned month) {
  static const uint8_t days[] = {31u, 28u, 31u, 30u, 31u, 30u, 31u, 31u, 30u, 31u, 30u, 31u};
  int leap;

  if (month == 0u || month > sizeof(days) / sizeof(days[0])) return 0u;
  leap = year % 4u == 0u && (year % 100u != 0u || year % 400u == 0u);
  return days[month - 1u] + (month == 2u && leap ? 1u : 0u);
}

static int flow_ocpp201_core_timestamp_valid(const json_value_t *value) {
  const char *text;
  size_t size;
  size_t index;
  unsigned year;
  unsigned month;
  unsigned day;
  unsigned hour;
  unsigned minute;
  unsigned second;
  unsigned offset_hour = 0u;
  unsigned offset_minute = 0u;

  if (!value || json_type(value) != JSON_STRING) return 0;
  text = json_string(value);
  size = json_string_len(value);
  if (!text || size < sizeof("2000-01-01T00:00:00Z") - 1u || text[4] != '-' || text[7] != '-' ||
      (text[10] != 'T' && text[10] != 't') || text[13] != ':' || text[16] != ':')
    return 0;
  for (index = 0u; index < 19u; ++index) {
    if (index == 4u || index == 7u || index == 10u || index == 13u || index == 16u) continue;
    if (!flow_ocpp201_core_ascii_digit(text[index])) return 0;
  }
  year = flow_ocpp201_core_decimal(text, 4u);
  month = flow_ocpp201_core_decimal(text + 5u, 2u);
  day = flow_ocpp201_core_decimal(text + 8u, 2u);
  hour = flow_ocpp201_core_decimal(text + 11u, 2u);
  minute = flow_ocpp201_core_decimal(text + 14u, 2u);
  second = flow_ocpp201_core_decimal(text + 17u, 2u);
  if (year == 0u || day == 0u || day > flow_ocpp201_core_days_in_month(year, month) || hour > 23u ||
      minute > 59u || second > 60u)
    return 0;

  index = 19u;
  if (index < size && text[index] == '.') {
    ++index;
    if (index == size || !flow_ocpp201_core_ascii_digit(text[index])) return 0;
    while (index < size && flow_ocpp201_core_ascii_digit(text[index]))
      ++index;
  }
  if (index < size && (text[index] == 'Z' || text[index] == 'z')) {
    ++index;
  } else {
    if (index + 6u != size || (text[index] != '+' && text[index] != '-') ||
        text[index + 3u] != ':' || !flow_ocpp201_core_ascii_digit(text[index + 1u]) ||
        !flow_ocpp201_core_ascii_digit(text[index + 2u]) ||
        !flow_ocpp201_core_ascii_digit(text[index + 4u]) ||
        !flow_ocpp201_core_ascii_digit(text[index + 5u]))
      return 0;
    offset_hour = flow_ocpp201_core_decimal(text + index + 1u, 2u);
    offset_minute = flow_ocpp201_core_decimal(text + index + 4u, 2u);
    index += 6u;
  }
  return index == size && offset_hour <= 23u && offset_minute <= 59u;
}

static int flow_ocpp201_core_parse_call(const turbo_flow_protocol_business_event_view_t *event,
                                        json_value_t **out_document, json_value_t **out_body) {
  json_value_t *document = NULL;
  json_value_t *message_type;
  json_value_t *correlation;
  json_value_t *operation;
  json_value_t *body;
  size_t operation_size;
  size_t correlation_size;
  int rc = SALTS_EPROTO;

  if (!event || !out_document || !out_body) return SALTS_EINVAL;
  *out_document = NULL;
  *out_body = NULL;
  if (event->metadata.message_type != 2u ||
      strcmp(event->metadata.protocol_version, FLOW_OCPP201_CORE_VERSION) != 0)
    return SALTS_ENOTSUP;
  document = json_parse((const char *)event->content.data, event->content.data_size);
  if (!document || json_type(document) != JSON_ARRAY || json_array_size(document) != 4u) goto done;

  message_type = json_array_get(document, 0u);
  correlation = json_array_get(document, 1u);
  operation = json_array_get(document, 2u);
  body = json_array_get(document, 3u);
  if (!message_type || json_type(message_type) != JSON_NUMBER || json_number(message_type) != 2.0 ||
      !correlation || json_type(correlation) != JSON_STRING || !operation ||
      json_type(operation) != JSON_STRING || !body || json_type(body) != JSON_OBJECT)
    goto done;

  operation_size = json_string_len(operation);
  correlation_size = json_string_len(correlation);
  if (operation_size != strlen(event->metadata.operation) ||
      memcmp(json_string(operation), event->metadata.operation, operation_size) != 0 ||
      correlation_size != strlen(event->metadata.correlation_id) ||
      memcmp(json_string(correlation), event->metadata.correlation_id, correlation_size) != 0)
    goto done;

  *out_document = document;
  *out_body = body;
  return SALTS_OK;

done:
  json_free(document);
  return rc;
}

static int flow_ocpp201_core_validate_boot(flow_ocpp201_core_owner_t *owner, json_value_t *body) {
  static const char reason_prefix[] = "{\"reason\":";
  ChargingStation_t station;
  BootNotificationReason_t reason;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  json_value_t *station_value;
  json_value_t *reason_value;
  char *station_json = NULL;
  char *reason_json = NULL;
  char reason_object[FLOW_OCPP201_CORE_REASON_JSON_MAX];
  size_t station_json_size = 0u;
  size_t reason_json_size = 0u;
  size_t reason_object_size;
  int rc;

  if (!owner || !body || json_type(body) != JSON_OBJECT || json_object_size(body) != 2u)
    return SALTS_EPROTO;
  station_value = json_object_get(body, "chargingStation");
  reason_value = json_object_get(body, "reason");
  if (!station_value || json_type(station_value) != JSON_OBJECT ||
      json_object_size(station_value) != 2u || !reason_value ||
      json_type(reason_value) != JSON_STRING)
    return SALTS_EPROTO;

  station_json = json_serialize(station_value, &station_json_size);
  reason_json = json_serialize(reason_value, &reason_json_size);
  if (!station_json || !reason_json) {
    rc = SALTS_ENOMEM;
    goto done_without_objects;
  }
  if (reason_json_size > sizeof(reason_object) - sizeof(reason_prefix)) {
    rc = SALTS_EPROTO;
    goto done_without_objects;
  }
  memcpy(reason_object, reason_prefix, sizeof(reason_prefix) - 1u);
  memcpy(reason_object + sizeof(reason_prefix) - 1u, reason_json, reason_json_size);
  reason_object_size = sizeof(reason_prefix) - 1u + reason_json_size;
  reason_object[reason_object_size++] = '}';

  ChargingStation_init(&station);
  BootNotificationReason_init(&reason);
  status =
      ChargingStation_from_json(owner->codec, &station, station_json, station_json_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc == SALTS_OK) {
    status = BootNotificationReason_from_json(owner->codec, &reason, reason_object,
                                              reason_object_size, &error);
    rc = flow_ocpp201_core_databind_status(status);
  }
  if (rc == SALTS_OK && (tstr_len(station.model) == 0u || tstr_len(station.vendorName) == 0u))
    rc = SALTS_EPROTO;
  BootNotificationReason_clear(&reason);
  ChargingStation_clear(&station);

done_without_objects:
  json_serialize_free(reason_json);
  json_serialize_free(station_json);
  return rc;
}

static int flow_ocpp201_core_validate_status(flow_ocpp201_core_owner_t *owner, json_value_t *body) {
  StatusNotificationRequest_t notification;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  json_value_t *timestamp;
  json_value_t *connector_status;
  json_value_t *evse_id;
  json_value_t *connector_id;
  char *body_json = NULL;
  size_t body_json_size = 0u;
  int rc;

  if (!owner || !body || json_type(body) != JSON_OBJECT || json_object_size(body) != 4u)
    return SALTS_EPROTO;
  timestamp = json_object_get(body, "timestamp");
  connector_status = json_object_get(body, "connectorStatus");
  evse_id = json_object_get(body, "evseId");
  connector_id = json_object_get(body, "connectorId");
  if (!flow_ocpp201_core_timestamp_valid(timestamp) || !connector_status ||
      json_type(connector_status) != JSON_STRING || !evse_id || json_type(evse_id) != JSON_NUMBER ||
      !connector_id || json_type(connector_id) != JSON_NUMBER)
    return SALTS_EPROTO;

  body_json = json_serialize(body, &body_json_size);
  if (!body_json) return SALTS_ENOMEM;
  StatusNotificationRequest_init(&notification);
  status = StatusNotificationRequest_from_json(owner->codec, &notification, body_json,
                                               body_json_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc == SALTS_OK && (notification.evseId == 0u || notification.connectorId == 0u ||
                         tstr_len(notification.timestamp) == 0u))
    rc = SALTS_EPROTO;
  StatusNotificationRequest_clear(&notification);
  json_serialize_free(body_json);
  return rc;
}

static int flow_ocpp201_core_json_add_clone(json_value_t *object, const char *key,
                                            const json_value_t *value) {
  json_value_t *copy;

  if (!object || !key || !value) return SALTS_EINVAL;
  copy = json_clone(value);
  if (!copy) return SALTS_ENOMEM;
  if (!json_object_add_checked(object, key, copy)) {
    json_free(copy);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int flow_ocpp201_core_json_project(const char *const *keys, json_value_t *const *values,
                                          size_t count, char **out, size_t *out_size) {
  json_value_t *object = NULL;
  size_t index;
  int rc = SALTS_OK;

  if (!keys || !values || count == 0u || !out || !out_size) return SALTS_EINVAL;
  *out = NULL;
  *out_size = 0u;
  object = json_create_object();
  if (!object) return SALTS_ENOMEM;
  for (index = 0u; index < count; ++index) {
    rc = flow_ocpp201_core_json_add_clone(object, keys[index], values[index]);
    if (rc != SALTS_OK) goto done;
  }
  *out = json_serialize(object, out_size);
  if (!*out) rc = SALTS_ENOMEM;

done:
  json_free(object);
  return rc;
}

static int flow_ocpp201_core_presence_has(const uint8_t *presence, size_t presence_size,
                                          size_t bit) {
  const size_t byte_index = bit / 8u;
  const uint8_t bit_mask = (uint8_t)(1u << (bit % 8u));
  return presence && byte_index < presence_size && (presence[byte_index] & bit_mask) != 0u;
}

static int flow_ocpp201_core_validate_transaction_evse(flow_ocpp201_core_owner_t *owner,
                                                       json_value_t *evse) {
  json_value_t *id;
  json_value_t *connector;
  TransactionEvseFacts_t facts;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  char *json = NULL;
  size_t json_size = 0u;
  int has_connector;
  int rc;

  if (!owner || !evse || json_type(evse) != JSON_OBJECT) return SALTS_EPROTO;
  id = json_object_get(evse, "id");
  connector = json_object_get(evse, "connectorId");
  if (!id || json_type(id) != JSON_NUMBER || (connector && json_type(connector) != JSON_NUMBER) ||
      json_object_size(evse) != 1u + (connector ? 1u : 0u))
    return SALTS_EPROTO;

  json = json_serialize(evse, &json_size);
  if (!json) return SALTS_ENOMEM;
  TransactionEvseFacts_init(&facts);
  status = TransactionEvseFacts_from_json(owner->codec, &facts, json, json_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  has_connector = flow_ocpp201_core_presence_has(facts._presence, sizeof(facts._presence),
                                                 TransactionEvseFacts_OPTIONAL_connectorId);
  if (rc == SALTS_OK && (facts.id == 0u || has_connector != (connector != NULL) ||
                         (has_connector && facts.connectorId == 0u)))
    rc = SALTS_EPROTO;
  TransactionEvseFacts_clear(&facts);
  json_serialize_free(json);
  return rc;
}

static int flow_ocpp201_core_validate_transaction_id_token(flow_ocpp201_core_owner_t *owner,
                                                           json_value_t *id_token) {
  TransactionIdToken_t token;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  json_value_t *value;
  json_value_t *type;
  char *json = NULL;
  size_t json_size = 0u;
  int rc;

  if (!owner || !id_token || json_type(id_token) != JSON_OBJECT || json_object_size(id_token) != 2u)
    return SALTS_EPROTO;
  value = json_object_get(id_token, "idToken");
  type = json_object_get(id_token, "type");
  if (!value || json_type(value) != JSON_STRING || !type || json_type(type) != JSON_STRING)
    return SALTS_EPROTO;
  json = json_serialize(id_token, &json_size);
  if (!json) return SALTS_ENOMEM;
  TransactionIdToken_init(&token);
  status = TransactionIdToken_from_json(owner->codec, &token, json, json_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc == SALTS_OK && (tstr_len(token.idToken) == 0u || tstr_len(token.idToken) > 36u))
    rc = SALTS_EPROTO;
  TransactionIdToken_clear(&token);
  json_serialize_free(json);
  return rc;
}

static int flow_ocpp201_core_validate_authorize(flow_ocpp201_core_owner_t *owner,
                                                json_value_t *body) {
  json_value_t *id_token;

  if (!owner || !body || json_type(body) != JSON_OBJECT || json_object_size(body) != 1u)
    return SALTS_EPROTO;
  id_token = json_object_get(body, "idToken");
  if (!id_token) return SALTS_EPROTO;
  return flow_ocpp201_core_validate_transaction_id_token(owner, id_token);
}

static int flow_ocpp201_core_validate_transaction(flow_ocpp201_core_owner_t *owner,
                                                  json_value_t *body) {
  static const char *const event_keys[] = {"eventType", "triggerReason", "seqNo", "timestamp",
                                           "offline"};
  json_value_t *event_values[5];
  TransactionEventFacts_t event;
  TransactionInfoRequired_t transaction;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  json_value_t *event_type;
  json_value_t *timestamp;
  json_value_t *trigger_reason;
  json_value_t *sequence;
  json_value_t *transaction_info;
  json_value_t *transaction_id;
  json_value_t *offline;
  json_value_t *evse;
  json_value_t *id_token;
  char *event_json = NULL;
  char *transaction_json = NULL;
  size_t event_json_size = 0u;
  size_t transaction_json_size = 0u;
  size_t event_field_count = 4u;
  int has_offline;
  int rc;

  if (!owner || !body || json_type(body) != JSON_OBJECT) return SALTS_EPROTO;
  event_type = json_object_get(body, "eventType");
  timestamp = json_object_get(body, "timestamp");
  trigger_reason = json_object_get(body, "triggerReason");
  sequence = json_object_get(body, "seqNo");
  transaction_info = json_object_get(body, "transactionInfo");
  offline = json_object_get(body, "offline");
  evse = json_object_get(body, "evse");
  id_token = json_object_get(body, "idToken");
  transaction_id = transaction_info && json_type(transaction_info) == JSON_OBJECT
                       ? json_object_get(transaction_info, "transactionId")
                       : NULL;
  if (!event_type || json_type(event_type) != JSON_STRING ||
      !flow_ocpp201_core_timestamp_valid(timestamp) || !trigger_reason ||
      json_type(trigger_reason) != JSON_STRING || !sequence || json_type(sequence) != JSON_NUMBER ||
      !transaction_info || json_type(transaction_info) != JSON_OBJECT ||
      json_object_size(transaction_info) != 1u || !transaction_id ||
      json_type(transaction_id) != JSON_STRING || (offline && json_type(offline) != JSON_BOOL) ||
      json_object_size(body) != 5u + (offline ? 1u : 0u) + (evse ? 1u : 0u) + (id_token ? 1u : 0u))
    return SALTS_EPROTO;

  event_values[0] = event_type;
  event_values[1] = trigger_reason;
  event_values[2] = sequence;
  event_values[3] = timestamp;
  if (offline) event_values[event_field_count++] = offline;
  rc = flow_ocpp201_core_json_project(event_keys, event_values, event_field_count, &event_json,
                                      &event_json_size);
  if (rc != SALTS_OK) return rc;

  transaction_json = json_serialize(transaction_info, &transaction_json_size);
  if (!transaction_json) {
    rc = SALTS_ENOMEM;
    goto done_without_objects;
  }

  TransactionEventFacts_init(&event);
  TransactionInfoRequired_init(&transaction);
  status =
      TransactionEventFacts_from_json(owner->codec, &event, event_json, event_json_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc == SALTS_OK) {
    status = TransactionInfoRequired_from_json(owner->codec, &transaction, transaction_json,
                                               transaction_json_size, &error);
    rc = flow_ocpp201_core_databind_status(status);
  }
  has_offline = flow_ocpp201_core_presence_has(event._presence, sizeof(event._presence),
                                               TransactionEventFacts_OPTIONAL_offline);
  if (rc == SALTS_OK &&
      (has_offline != (offline != NULL) || tstr_len(event.timestamp) == 0u ||
       tstr_len(transaction.transactionId) == 0u || tstr_len(transaction.transactionId) > 36u))
    rc = SALTS_EPROTO;
  TransactionInfoRequired_clear(&transaction);
  TransactionEventFacts_clear(&event);
  if (rc == SALTS_OK && evse) rc = flow_ocpp201_core_validate_transaction_evse(owner, evse);
  if (rc == SALTS_OK && id_token)
    rc = flow_ocpp201_core_validate_transaction_id_token(owner, id_token);

done_without_objects:
  json_serialize_free(transaction_json);
  json_serialize_free(event_json);
  return rc;
}

static int flow_ocpp201_core_consume(void *ctx,
                                     const turbo_flow_protocol_business_event_view_t *event) {
  flow_ocpp201_core_owner_t *owner = (flow_ocpp201_core_owner_t *)ctx;
  json_value_t *document = NULL;
  json_value_t *body = NULL;
  int rc;

  if (!owner || !owner->codec || !event) return SALTS_EINVAL;
  rc = flow_ocpp201_core_content_is_raw_json(&event->content);
  if (rc != SALTS_OK) return rc;
  rc = flow_ocpp201_core_parse_call(event, &document, &body);
  if (rc != SALTS_OK) return rc;

  if (strcmp(event->metadata.operation, "Heartbeat") == 0) {
    rc = json_object_size(body) == 0u ? SALTS_OK : SALTS_EPROTO;
    goto done;
  }
  if (strcmp(event->metadata.operation, "BootNotification") == 0) {
    rc = flow_ocpp201_core_validate_boot(owner, body);
  } else if (strcmp(event->metadata.operation, "Authorize") == 0) {
    rc = flow_ocpp201_core_validate_authorize(owner, body);
  } else if (strcmp(event->metadata.operation, "StatusNotification") == 0) {
    rc = flow_ocpp201_core_validate_status(owner, body);
  } else if (strcmp(event->metadata.operation, "TransactionEvent") == 0) {
    rc = flow_ocpp201_core_validate_transaction(owner, body);
  } else {
    rc = SALTS_ENOTSUP;
  }

done:
  json_free(document);
  return rc;
}

static int
flow_ocpp201_core_write_command(const turbo_flow_protocol_business_command_request_t *request,
                                const char *operation, const char *json, size_t json_size,
                                turbo_flow_protocol_business_command_output_t *output) {
  int rc;

  if (!request || !operation || !json || !output) return SALTS_EINVAL;
  if (json_size > output->payload_capacity) return SALTS_EMSGSIZE;
  rc =
      flow_protocol_metadata_text(output->device_id, sizeof(output->device_id), request->device_id);
  if (rc == SALTS_OK)
    rc = flow_protocol_metadata_text(output->operation, sizeof(output->operation), operation);
  if (rc == SALTS_OK)
    rc = flow_protocol_metadata_text(output->correlation_id, sizeof(output->correlation_id),
                                     request->correlation_id);
  if (rc != SALTS_OK) return rc;
  if (json_size > 0u) memcpy(output->payload, json, json_size);
  output->payload_size = json_size;
  output->sequence = request->sequence;
  return SALTS_OK;
}

static int
flow_ocpp201_core_prepare_reset(flow_ocpp201_core_owner_t *owner,
                                const turbo_flow_protocol_business_command_request_t *request,
                                turbo_flow_protocol_business_command_output_t *output) {
  json_value_t *document = NULL;
  json_value_t *type_value;
  ResetRequest_t reset;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  const char *json;
  size_t json_size;
  int rc;

  rc = flow_ocpp201_core_content_is_typed_json(&request->content, "ResetRequest");
  if (rc != SALTS_OK) return rc;
  document = json_parse((const char *)request->content.data, request->content.data_size);
  if (!document || json_type(document) != JSON_OBJECT || json_object_size(document) != 1u) {
    rc = SALTS_EPROTO;
    goto done_without_reset;
  }
  type_value = json_object_get(document, "type");
  if (!type_value || json_type(type_value) != JSON_STRING) {
    rc = SALTS_EPROTO;
    goto done_without_reset;
  }

  ResetRequest_init(&reset);
  status = ResetRequest_from_json(owner->codec, &reset, (const char *)request->content.data,
                                  request->content.data_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc != SALTS_OK) goto done;
  if (reset.type == ResetType_Immediate) {
    json = "{\"type\":\"Immediate\"}";
  } else if (reset.type == ResetType_OnIdle) {
    json = "{\"type\":\"OnIdle\"}";
  } else {
    rc = SALTS_EPROTO;
    goto done;
  }
  json_size = strlen(json);
  rc = flow_ocpp201_core_write_command(request, "Reset", json, json_size, output);

done:
  if (rc != SALTS_OK) output->payload_size = 0u;
  ResetRequest_clear(&reset);
done_without_reset:
  json_free(document);
  return rc;
}

static int
flow_ocpp201_core_prepare_unlock(flow_ocpp201_core_owner_t *owner,
                                 const turbo_flow_protocol_business_command_request_t *request,
                                 turbo_flow_protocol_business_command_output_t *output) {
  json_value_t *document = NULL;
  json_value_t *evse_id;
  json_value_t *connector_id;
  UnlockConnectorRequest_t unlock;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  char *json = NULL;
  size_t json_size = 0u;
  int rc;

  rc = flow_ocpp201_core_content_is_typed_json(&request->content, "UnlockConnectorRequest");
  if (rc != SALTS_OK) return rc;
  document = json_parse((const char *)request->content.data, request->content.data_size);
  if (!document || json_type(document) != JSON_OBJECT || json_object_size(document) != 2u) {
    rc = SALTS_EPROTO;
    goto done_without_unlock;
  }
  evse_id = json_object_get(document, "evseId");
  connector_id = json_object_get(document, "connectorId");
  if (!evse_id || json_type(evse_id) != JSON_NUMBER || !connector_id ||
      json_type(connector_id) != JSON_NUMBER) {
    rc = SALTS_EPROTO;
    goto done_without_unlock;
  }

  UnlockConnectorRequest_init(&unlock);
  status =
      UnlockConnectorRequest_from_json(owner->codec, &unlock, (const char *)request->content.data,
                                       request->content.data_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc != SALTS_OK) goto done;
  if (unlock.evseId == 0u || unlock.connectorId == 0u) {
    rc = SALTS_EPROTO;
    goto done;
  }
  status = UnlockConnectorRequest_to_json(owner->codec, &unlock, &json, &json_size, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc == SALTS_OK)
    rc = flow_ocpp201_core_write_command(request, "UnlockConnector", json, json_size, output);

done:
  if (rc != SALTS_OK) output->payload_size = 0u;
  tbe_typed_serialized_free(json);
  UnlockConnectorRequest_clear(&unlock);
done_without_unlock:
  json_free(document);
  return rc;
}

static int flow_ocpp201_core_prepare(void *ctx,
                                     const turbo_flow_protocol_business_command_request_t *request,
                                     turbo_flow_protocol_business_command_output_t *output) {
  flow_ocpp201_core_owner_t *owner = (flow_ocpp201_core_owner_t *)ctx;

  if (!owner || !owner->codec || !request || !output) return SALTS_EINVAL;
  if (!request->correlation_id || request->correlation_id[0] == '\0') return SALTS_EINVAL;
  if (strcmp(request->action, "reset") == 0)
    return flow_ocpp201_core_prepare_reset(owner, request, output);
  if (strcmp(request->action, "unlock-connector") == 0)
    return flow_ocpp201_core_prepare_unlock(owner, request, output);
  return SALTS_ENOTSUP;
}

static int flow_ocpp201_core_open(void *ctx,
                                  const turbo_flow_protocol_business_open_request_t *request,
                                  turbo_flow_protocol_business_service_t *service) {
  flow_ocpp201_core_owner_t *owner;
  turbo_flow_protocol_business_ops_t ops = TURBO_FLOW_PROTOCOL_BUSINESS_OPS_INIT;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  int rc;

  (void)ctx;
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION ||
      request->protocol != TURBO_FLOW_PROTOCOL_OCPP || !request->profile ||
      request->max_payload_size == 0u || !service || service->size < sizeof(*service) ||
      service->abi_version != TURBO_FLOW_PROTOCOL_BUSINESS_ABI_VERSION)
    return SALTS_EINVAL;
  if (strcmp(request->profile, FLOW_OCPP201_CORE_PROFILE) != 0) return SALTS_ENOTSUP;
  if (data_bind_abi_version() != DATA_BIND_ABI_VERSION) return SALTS_EPROTONOSUPPORT;

  owner = (flow_ocpp201_core_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  status = Ocpp201Core_codec_create(&owner->codec, &error);
  rc = flow_ocpp201_core_databind_status(status);
  if (rc != SALTS_OK) goto fail;

  ops.consume_committed = flow_ocpp201_core_consume;
  ops.prepare_command = flow_ocpp201_core_prepare;
  rc = turbo_flow_protocol_business_create(FLOW_OCPP201_CORE_BUSINESS, TURBO_FLOW_PROTOCOL_OCPP,
                                           FLOW_OCPP201_CORE_PROFILE, request->max_payload_size,
                                           TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT |
                                               TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND,
                                           &ops, owner, &owner->business);
  if (rc != SALTS_OK) goto fail;

  service->protocol = TURBO_FLOW_PROTOCOL_OCPP;
  service->instance = owner->business;
  service->owner = owner;
  return SALTS_OK;

fail:
  data_bind_free(owner->codec);
  free(owner);
  return rc;
}

static void flow_ocpp201_core_close(void *ctx, turbo_flow_protocol_business_service_t *service) {
  flow_ocpp201_core_owner_t *owner;

  (void)ctx;
  if (!service) return;
  owner = (flow_ocpp201_core_owner_t *)service->owner;
  if (owner) {
    turbo_flow_protocol_business_destroy(owner->business);
    data_bind_free(owner->codec);
    free(owner);
  }
  service->instance = NULL;
  service->owner = NULL;
}

static const turbo_flow_protocol_business_plugin_api_t FLOW_OCPP201_CORE_API = {
    sizeof(turbo_flow_protocol_business_plugin_api_t),
    TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_BUSINESS_PLUGIN_API_VERSION_MINOR,
    FLOW_OCPP201_CORE_BUSINESS,
    TURBO_FLOW_PROTOCOL_OCPP,
    TURBO_FLOW_PROTOCOL_BUSINESS_CAP_COMMITTED_EVENT |
        TURBO_FLOW_PROTOCOL_BUSINESS_CAP_PREPARE_COMMAND,
    NULL,
    flow_ocpp201_core_open,
    flow_ocpp201_core_close};

FLOW_PROTOCOL_BUSINESS_DEFINE_UNIFIED_ROOT("ocpp201-core", "1.0.0", FLOW_OCPP201_CORE_API)
