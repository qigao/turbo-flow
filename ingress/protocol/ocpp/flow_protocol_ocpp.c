#include "flow_protocol_plugin_support.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

static int flow_ocpp_action_supported(const char *version, const char *action, size_t action_size) {
  static const char *const v16[] = {"Authorize",
                                    "BootNotification",
                                    "CancelReservation",
                                    "ChangeAvailability",
                                    "ChangeConfiguration",
                                    "ClearCache",
                                    "ClearChargingProfile",
                                    "DataTransfer",
                                    "DiagnosticsStatusNotification",
                                    "FirmwareStatusNotification",
                                    "GetCompositeSchedule",
                                    "GetConfiguration",
                                    "GetDiagnostics",
                                    "GetLocalListVersion",
                                    "Heartbeat",
                                    "MeterValues",
                                    "RemoteStartTransaction",
                                    "RemoteStopTransaction",
                                    "ReserveNow",
                                    "Reset",
                                    "SendLocalList",
                                    "SetChargingProfile",
                                    "StartTransaction",
                                    "StatusNotification",
                                    "StopTransaction",
                                    "TriggerMessage",
                                    "UnlockConnector",
                                    "UpdateFirmware"};
  static const char *const v201[] = {"Authorize",
                                     "BootNotification",
                                     "CancelReservation",
                                     "CertificateSigned",
                                     "ChangeAvailability",
                                     "ClearCache",
                                     "ClearChargingProfile",
                                     "ClearDisplayMessage",
                                     "ClearedChargingLimit",
                                     "CostUpdated",
                                     "CustomerInformation",
                                     "DataTransfer",
                                     "DeleteCertificate",
                                     "FirmwareStatusNotification",
                                     "Get15118EVCertificate",
                                     "GetBaseReport",
                                     "GetCertificateStatus",
                                     "GetChargingProfiles",
                                     "GetCompositeSchedule",
                                     "GetDisplayMessages",
                                     "GetInstalledCertificateIds",
                                     "GetLocalListVersion",
                                     "GetLog",
                                     "GetMonitoringReport",
                                     "GetReport",
                                     "GetTransactionStatus",
                                     "GetVariables",
                                     "Heartbeat",
                                     "InstallCertificate",
                                     "LogStatusNotification",
                                     "MeterValues",
                                     "NotifyChargingLimit",
                                     "NotifyCustomerInformation",
                                     "NotifyDisplayMessages",
                                     "NotifyEVChargingNeeds",
                                     "NotifyEVChargingSchedule",
                                     "NotifyEvent",
                                     "NotifyMonitoringReport",
                                     "NotifyReport",
                                     "PublishFirmware",
                                     "PublishFirmwareStatusNotification",
                                     "ReportChargingProfiles",
                                     "RequestStartTransaction",
                                     "RequestStopTransaction",
                                     "ReservationStatusUpdate",
                                     "ReserveNow",
                                     "Reset",
                                     "SecurityEventNotification",
                                     "SendLocalList",
                                     "SetChargingProfile",
                                     "SetDisplayMessage",
                                     "SetMonitoringBase",
                                     "SetMonitoringLevel",
                                     "SetNetworkProfile",
                                     "SetVariableMonitoring",
                                     "SetVariables",
                                     "SignCertificate",
                                     "StatusNotification",
                                     "TransactionEvent",
                                     "TriggerMessage",
                                     "UnlockConnector",
                                     "UnpublishFirmware",
                                     "UpdateFirmware"};
  const char *const *actions;
  size_t count;
  if (!version || !action || action_size == 0u) return 0;
  if (strcmp(version, "1.6J") == 0) {
    actions = v16;
    count = sizeof(v16) / sizeof(v16[0]);
  } else if (strcmp(version, "2.0.1") == 0) {
    actions = v201;
    count = sizeof(v201) / sizeof(v201[0]);
  } else {
    return 0;
  }
  for (size_t i = 0u; i < count; ++i)
    if (strlen(actions[i]) == action_size && memcmp(actions[i], action, action_size) == 0) return 1;
  return 0;
}

static int flow_ocpp_inspect(void *ctx, const char *configured_version,
                             const turbo_flow_protocol_frame_view_t *frame,
                             turbo_flow_protocol_metadata_t *metadata) {
  turbo_json_doc_t *document = NULL;
  json_value_t *message_type_value;
  json_value_t *correlation_value;
  json_value_t *operation_value = NULL;
  double message_type_number;
  unsigned message_type;
  const char *operation;
  int rc = TURBO_EPROTO;
  (void)ctx;
  if (!frame || !metadata ||
      turbo_parse_json(frame->data, frame->data_size, &document) != TURBO_OK || !document ||
      turbo_json_type(document) != TURBO_JSON_ARRAY)
    goto done;
  if (turbo_json_array_size(document) < 3u) goto done;
  message_type_value = turbo_json_array_get(document, 0u);
  correlation_value = turbo_json_array_get(document, 1u);
  if (!message_type_value || turbo_json_type(message_type_value) != TURBO_JSON_NUMBER ||
      !correlation_value || turbo_json_type(correlation_value) != TURBO_JSON_STRING)
    goto done;
  message_type_number = turbo_json_number(message_type_value);
  if (!isfinite(message_type_number) || message_type_number != floor(message_type_number) ||
      message_type_number < 2.0 || message_type_number > 4.0)
    goto done;
  message_type = (unsigned)message_type_number;
  if (turbo_json_string_len(correlation_value) == 0u ||
      turbo_json_string_len(correlation_value) > TURBO_FLOW_PROTOCOL_CORRELATION_MAX)
    goto done;
  if (flow_protocol_metadata_text_n(metadata->correlation_id, sizeof(metadata->correlation_id),
                                   turbo_json_string(correlation_value),
                                   turbo_json_string_len(correlation_value)) != TURBO_OK)
    goto done;
  if (message_type == 2u) {
    if (turbo_json_array_size(document) != 4u) goto done;
    operation_value = turbo_json_array_get(document, 2u);
    if (!operation_value || turbo_json_type(operation_value) != TURBO_JSON_STRING ||
        turbo_json_string_len(operation_value) == 0u ||
        turbo_json_string_len(operation_value) > TURBO_FLOW_PROTOCOL_OPERATION_MAX)
      goto done;
    if (!flow_ocpp_action_supported(configured_version, turbo_json_string(operation_value),
                                    turbo_json_string_len(operation_value)))
      goto done;
    if (!turbo_json_array_get(document, 3u) ||
        turbo_json_type(turbo_json_array_get(document, 3u)) != TURBO_JSON_OBJECT)
      goto done;
    operation = turbo_json_string(operation_value);
  } else if (message_type == 3u) {
    if (turbo_json_array_size(document) != 3u || !turbo_json_array_get(document, 2u) ||
        turbo_json_type(turbo_json_array_get(document, 2u)) != TURBO_JSON_OBJECT)
      goto done;
    operation = "call-result";
  } else {
    if (turbo_json_array_size(document) != 5u || !turbo_json_array_get(document, 2u) ||
        turbo_json_type(turbo_json_array_get(document, 2u)) != TURBO_JSON_STRING ||
        !turbo_json_array_get(document, 3u) ||
        turbo_json_type(turbo_json_array_get(document, 3u)) != TURBO_JSON_STRING ||
        !turbo_json_array_get(document, 4u) ||
        turbo_json_type(turbo_json_array_get(document, 4u)) != TURBO_JSON_OBJECT)
      goto done;
    operation = message_type == 3u ? "call-result" : "call-error";
  }
  metadata->message_type = message_type;
  if (message_type == 2u)
    rc = flow_protocol_metadata_text_n(metadata->operation, sizeof(metadata->operation), operation,
                                      turbo_json_string_len(operation_value));
  else rc = flow_protocol_metadata_text(metadata->operation, sizeof(metadata->operation), operation);

done:
  turbo_free_json(&document);
  return rc;
}

typedef struct flow_ocpp_writer_s {
  uint8_t *data;
  size_t capacity;
  size_t size;
} flow_ocpp_writer_t;

static int flow_ocpp_write(flow_ocpp_writer_t *writer, const void *data, size_t size) {
  if (!writer || (!data && size != 0u)) return TURBO_EINVAL;
  if (size > writer->capacity - writer->size) return TURBO_EMSGSIZE;
  if (size > 0u) memcpy(writer->data + writer->size, data, size);
  writer->size += size;
  return TURBO_OK;
}

static int flow_ocpp_write_string(flow_ocpp_writer_t *writer, const char *text, size_t size) {
  static const char quote = '"';
  int rc = flow_ocpp_write(writer, &quote, 1u);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < size; ++i) {
    const unsigned char value = (unsigned char)text[i];
    const char *escape = NULL;
    char encoded[7];
    size_t encoded_size = 0u;
    switch (value) {
    case '"':
      escape = "\\\"";
      break;
    case '\\':
      escape = "\\\\";
      break;
    case '\b':
      escape = "\\b";
      break;
    case '\f':
      escape = "\\f";
      break;
    case '\n':
      escape = "\\n";
      break;
    case '\r':
      escape = "\\r";
      break;
    case '\t':
      escape = "\\t";
      break;
    default:
      break;
    }
    if (escape) {
      rc = flow_ocpp_write(writer, escape, 2u);
    } else if (value < 0x20u) {
      (void)snprintf(encoded, sizeof(encoded), "\\u%04x", value);
      encoded_size = sizeof(encoded) - 1u;
      rc = flow_ocpp_write(writer, encoded, encoded_size);
    } else {
      rc = flow_ocpp_write(writer, text + i, 1u);
    }
    if (rc != TURBO_OK) return rc;
  }
  return flow_ocpp_write(writer, &quote, 1u);
}

static int flow_ocpp_payload_validate(const uint8_t *payload, size_t payload_size) {
  turbo_json_doc_t *document = NULL;
  int rc = TURBO_EPROTO;
  if (!payload || payload_size == 0u) return TURBO_EINVAL;
  if (turbo_parse_json(payload, payload_size, &document) == TURBO_OK && document &&
      turbo_json_type(document) == TURBO_JSON_OBJECT)
    rc = TURBO_OK;
  turbo_free_json(&document);
  return rc;
}

static int flow_ocpp_encode_envelope(unsigned message_type, const char *correlation_id,
                                     const char *operation, const char *error_description,
                                     const uint8_t *payload, size_t payload_size,
                                     turbo_flow_protocol_frame_output_t *output) {
  flow_ocpp_writer_t writer;
  int rc;
  if (!correlation_id || !correlation_id[0] || !output || !output->data) return TURBO_EINVAL;
  rc = flow_ocpp_payload_validate(payload, payload_size);
  if (rc != TURBO_OK) return rc;
  writer.data = output->data;
  writer.capacity = output->capacity;
  writer.size = 0u;
  rc = flow_ocpp_write(&writer,
                       message_type == 2u   ? "[2,"
                       : message_type == 3u ? "[3,"
                                            : "[4,",
                       3u);
  if (rc == TURBO_OK) rc = flow_ocpp_write_string(&writer, correlation_id, strlen(correlation_id));
  if (rc == TURBO_OK && message_type == 2u) {
    rc = flow_ocpp_write(&writer, ",", 1u);
    if (rc == TURBO_OK) rc = flow_ocpp_write_string(&writer, operation, strlen(operation));
  } else if (rc == TURBO_OK && message_type == 4u) {
    rc = flow_ocpp_write(&writer, ",", 1u);
    if (rc == TURBO_OK) rc = flow_ocpp_write_string(&writer, operation, strlen(operation));
    if (rc == TURBO_OK) rc = flow_ocpp_write(&writer, ",", 1u);
    if (rc == TURBO_OK)
      rc = flow_ocpp_write_string(&writer, error_description ? error_description : "",
                                  error_description ? strlen(error_description) : 0u);
  }
  if (rc == TURBO_OK) rc = flow_ocpp_write(&writer, ",", 1u);
  if (rc == TURBO_OK) rc = flow_ocpp_write(&writer, payload, payload_size);
  if (rc == TURBO_OK) rc = flow_ocpp_write(&writer, "]", 1u);
  if (rc != TURBO_OK) return rc;
  output->data_size = writer.size;
  return TURBO_OK;
}

static int flow_ocpp_reply(void *ctx, const char *configured_version,
                           const turbo_flow_protocol_frame_view_t *request, int status,
                           turbo_flow_protocol_frame_output_t *output) {
  static const uint8_t empty_object[] = "{}";
  turbo_json_doc_t *document = NULL;
  json_value_t *type_value;
  json_value_t *correlation;
  double type_number;
  int rc = TURBO_EPROTO;
  (void)ctx;
  (void)configured_version;
  if (!request || !output ||
      turbo_parse_json(request->data, request->data_size, &document) != TURBO_OK || !document ||
      turbo_json_type(document) != TURBO_JSON_ARRAY)
    goto done;
  type_value = turbo_json_array_get(document, 0u);
  correlation = turbo_json_array_get(document, 1u);
  if (!type_value || turbo_json_type(type_value) != TURBO_JSON_NUMBER || !correlation ||
      turbo_json_type(correlation) != TURBO_JSON_STRING)
    goto done;
  type_number = turbo_json_number(type_value);
  if (type_number != 2.0) {
    output->data_size = 0u;
    rc = TURBO_OK;
    goto done;
  }
  rc = flow_ocpp_encode_envelope(status == TURBO_OK ? 3u : 4u, turbo_json_string(correlation),
                                 status == TURBO_OK ? NULL : "InternalError",
                                 status == TURBO_OK ? NULL : "Flow settlement failed", empty_object,
                                 sizeof(empty_object) - 1u, output);
done:
  turbo_free_json(&document);
  return rc;
}

static int flow_ocpp_encode(void *ctx, const char *configured_version,
                            const turbo_flow_protocol_command_view_t *command,
                            turbo_flow_protocol_frame_output_t *output) {
  unsigned message_type = 2u;
  const char *wire_operation;
  (void)ctx;
  if (!command || !command->correlation_id) return TURBO_EINVAL;
  if (strcmp(command->operation, "call-result") == 0) {
    message_type = 3u;
    wire_operation = NULL;
  } else if (strcmp(command->operation, "call-error") == 0) {
    message_type = 4u;
    wire_operation = command->resource && command->resource[0] ? command->resource : "GenericError";
  } else {
    wire_operation = command->operation;
    if (!flow_ocpp_action_supported(configured_version, wire_operation, strlen(wire_operation)))
      return TURBO_ENOTSUP;
  }
  return flow_ocpp_encode_envelope(message_type, command->correlation_id, wire_operation,
                                   message_type == 4u ? "Command rejected" : NULL, command->payload,
                                   command->payload_size, output);
}

static const char *const FLOW_OCPP_VERSIONS[] = {"1.6J", "2.0.1"};
static const flow_protocol_plugin_descriptor_t FLOW_OCPP_DESCRIPTOR = {
    "ocpp",
    TURBO_FLOW_PROTOCOL_OCPP,
    "2.0.1",
    FLOW_OCPP_VERSIONS,
    sizeof(FLOW_OCPP_VERSIONS) / sizeof(FLOW_OCPP_VERSIONS[0]),
    flow_ocpp_inspect,
    flow_ocpp_reply,
    flow_ocpp_encode,
    NULL};

static const turbo_flow_protocol_plugin_api_t FLOW_OCPP_API = {
    sizeof(turbo_flow_protocol_plugin_api_t),
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
    "ocpp",
    TURBO_FLOW_PROTOCOL_OCPP,
    TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
        TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE,
    (void *)&FLOW_OCPP_DESCRIPTOR,
    flow_protocol_plugin_open,
    flow_protocol_plugin_close};

const turbo_flow_protocol_plugin_api_t *turbo_flow_protocol_plugin_get_api(void) {
  return &FLOW_OCPP_API;
}
