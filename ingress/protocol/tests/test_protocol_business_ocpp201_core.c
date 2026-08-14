#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_business.h"

#include <string.h>

#define OCPP201_CORE_BUSINESS "ocpp201-core"
#define OCPP201_CORE_PROFILE "ocpp-2.0.1-core-minimal"
#define OCPP201_CORE_SCHEMA_ID "Ocpp201Core"

static int ocpp201_core_business_open(turbo_flow_protocol_business_registry_t **out_registry,
                                      turbo_flow_protocol_business_owner_t **out_owner,
                                      turbo_flow_protocol_business_t **out_business) {
  turbo_flow_protocol_business_open_request_t request =
      TURBO_FLOW_PROTOCOL_BUSINESS_OPEN_REQUEST_INIT;
  turbo_flow_protocol_business_registry_t *registry = NULL;
  turbo_flow_protocol_business_owner_t *owner = NULL;
  char reason[256];
  int rc;

  if (!out_registry || !out_owner || !out_business) return TURBO_EINVAL;
  *out_registry = NULL;
  *out_owner = NULL;
  *out_business = NULL;
  request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
  request.profile = OCPP201_CORE_PROFILE;
  request.max_payload_size = 4096u;

  rc = turbo_flow_protocol_business_registry_create(1u, &registry);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_protocol_business_registry_load(
      registry, FLOW_PROTOCOL_BUSINESS_OCPP201_CORE_MODULE, reason, sizeof(reason));
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_business_owner_create_registered(registry, OCPP201_CORE_BUSINESS,
                                                             &request, &owner);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_business_owner_instance(owner, TURBO_FLOW_PROTOCOL_OCPP,
                                                    out_business);
  if (rc != TURBO_OK) {
    turbo_flow_protocol_business_owner_destroy(owner);
    (void)turbo_flow_protocol_business_registry_destroy(registry);
    return rc;
  }
  *out_registry = registry;
  *out_owner = owner;
  return TURBO_OK;
}

static void ocpp201_core_business_close(turbo_flow_protocol_business_registry_t *registry,
                                        turbo_flow_protocol_business_owner_t *owner) {
  turbo_flow_protocol_business_owner_destroy(owner);
  check_int_eq(turbo_flow_protocol_business_registry_destroy(registry), TURBO_OK);
}

static turbo_flow_protocol_business_event_view_t
ocpp201_core_event(uint64_t delivery_id, const uint8_t *payload, size_t payload_size,
                   const char *operation, const char *correlation_id) {
  turbo_flow_protocol_business_event_view_t event = TURBO_FLOW_PROTOCOL_BUSINESS_EVENT_VIEW_INIT;

  event.delivery_id = delivery_id;
  event.session_id = 7u;
  event.session_generation = 3u;
  event.route = "protocol/ocpp/charger-1/call";
  event.route_size = strlen(event.route);
  event.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
  event.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
  event.metadata.message_type = 2u;
  memcpy(event.metadata.protocol_version, "2.0.1", sizeof("2.0.1"));
  memcpy(event.metadata.device_id, "charger-1", sizeof("charger-1"));
  memcpy(event.metadata.operation, operation, strlen(operation) + 1u);
  memcpy(event.metadata.correlation_id, correlation_id, strlen(correlation_id) + 1u);
  event.content.data = payload;
  event.content.data_size = payload_size;
  event.content.media_type = "application/json";
  return event;
}

static int ocpp201_core_protocol_open(turbo_flow_protocol_registry_t **out_registry,
                                     turbo_flow_protocol_owner_t **out_owner,
                                     turbo_flow_protocol_t **out_protocol) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  char reason[256];
  int rc;

  if (!out_registry || !out_owner || !out_protocol) return TURBO_EINVAL;
  *out_registry = NULL;
  *out_owner = NULL;
  *out_protocol = NULL;
  request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
  request.protocol_version = "2.0.1";

  rc = turbo_flow_protocol_registry_create(1u, &registry);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_protocol_registry_load(registry, FLOW_PROTOCOL_OCPP_MODULE, reason, sizeof(reason));
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_owner_create_registered(registry, "ocpp", &request, &owner);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_owner_instance(owner, TURBO_FLOW_PROTOCOL_OCPP, out_protocol);
  if (rc != TURBO_OK) {
    turbo_flow_protocol_owner_destroy(owner);
    (void)turbo_flow_protocol_registry_destroy(registry);
    return rc;
  }
  *out_registry = registry;
  *out_owner = owner;
  return TURBO_OK;
}

spec("OCPP 2.0.1 Core DataBind business provider") {
  it("rejects an unsupported business profile before opening") {
    turbo_flow_protocol_business_open_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_OPEN_REQUEST_INIT;
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    char reason[256];

    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.profile = "ocpp-2.0.1-full";
    request.max_payload_size = 4096u;
    check_int_eq(turbo_flow_protocol_business_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_registry_load(
                     registry, FLOW_PROTOCOL_BUSINESS_OCPP201_CORE_MODULE, reason, sizeof(reason)),
                 TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_owner_create_registered(
                     registry, OCPP201_CORE_BUSINESS, &request, &owner),
                 TURBO_ENOTSUP);
    check_null(owner);
    check_int_eq(turbo_flow_protocol_business_registry_destroy(registry), TURBO_OK);
  }

  it("accepts a committed typed BootNotification body") {
    static const uint8_t frame[] = "[2,\"boot-1\",\"BootNotification\","
                                   "{\"chargingStation\":{\"model\":\"Model-A\","
                                   "\"vendorName\":\"Vendor-A\"},\"reason\":\"PowerUp\"}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(1u, frame, sizeof(frame) - 1u, "BootNotification", "boot-1");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects a BootNotification missing a required nested field") {
    static const uint8_t frame[] = "[2,\"boot-2\",\"BootNotification\","
                                   "{\"chargingStation\":{\"model\":\"Model-A\"},"
                                   "\"reason\":\"PowerUp\"}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(2u, frame, sizeof(frame) - 1u, "BootNotification", "boot-2");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("accepts only an empty Heartbeat body") {
    static const uint8_t valid[] = "[2,\"hb-1\",\"Heartbeat\",{}]";
    static const uint8_t invalid[] = "[2,\"hb-2\",\"Heartbeat\",{\"unexpected\":true}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(3u, valid, sizeof(valid) - 1u, "Heartbeat", "hb-1");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    event = ocpp201_core_event(4u, invalid, sizeof(invalid) - 1u, "Heartbeat", "hb-2");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("accepts a minimal typed Authorize identity") {
    static const uint8_t frame[] =
        "[2,\"auth-1\",\"Authorize\","
        "{\"idToken\":{\"idToken\":\"04AABBCCDD\",\"type\":\"ISO14443\"}}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(5u, frame, sizeof(frame) - 1u, "Authorize", "auth-1");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects unsupported or malformed Authorize identity facts") {
    static const uint8_t unsupported_field[] =
        "[2,\"auth-2\",\"Authorize\","
        "{\"idToken\":{\"idToken\":\"04AABBCCDD\",\"type\":\"ISO14443\"},"
        "\"certificate\":\"unbound\"}]";
    static const uint8_t invalid_type[] =
        "[2,\"auth-3\",\"Authorize\","
        "{\"idToken\":{\"idToken\":\"04AABBCCDD\",\"type\":\"Unknown\"}}]";
    static const uint8_t missing_token[] = "[2,\"auth-4\",\"Authorize\",{}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event;

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    event = ocpp201_core_event(6u, unsupported_field, sizeof(unsupported_field) - 1u,
                               "Authorize", "auth-2");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event =
        ocpp201_core_event(7u, invalid_type, sizeof(invalid_type) - 1u, "Authorize", "auth-3");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(8u, missing_token, sizeof(missing_token) - 1u, "Authorize",
                               "auth-4");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("accepts a typed StatusNotification with an RFC3339 timestamp") {
    static const uint8_t frame[] =
        "[2,\"status-1\",\"StatusNotification\","
        "{\"timestamp\":\"2026-07-27T10:15:30.125+08:00\","
        "\"connectorStatus\":\"Available\",\"evseId\":1,\"connectorId\":2}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(9u, frame, sizeof(frame) - 1u, "StatusNotification", "status-1");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects malformed StatusNotification state facts") {
    static const uint8_t invalid_timestamp[] =
        "[2,\"status-2\",\"StatusNotification\","
        "{\"timestamp\":\"2026-07-27 10:15:30\","
        "\"connectorStatus\":\"Occupied\",\"evseId\":1,\"connectorId\":2}]";
    static const uint8_t invalid_status[] =
        "[2,\"status-3\",\"StatusNotification\","
        "{\"timestamp\":\"2026-07-27T10:15:30Z\","
        "\"connectorStatus\":\"Charging\",\"evseId\":1,\"connectorId\":2}]";
    static const uint8_t invalid_identity[] =
        "[2,\"status-4\",\"StatusNotification\","
        "{\"timestamp\":\"2026-07-27T10:15:30Z\","
        "\"connectorStatus\":\"Faulted\",\"evseId\":0,\"connectorId\":2}]";
    static const uint8_t invalid_calendar[] =
        "[2,\"status-5\",\"StatusNotification\","
        "{\"timestamp\":\"2026-02-30T10:15:30+24:00\","
        "\"connectorStatus\":\"Reserved\",\"evseId\":1,\"connectorId\":2}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event;

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    event = ocpp201_core_event(6u, invalid_timestamp, sizeof(invalid_timestamp) - 1u,
                               "StatusNotification", "status-2");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(7u, invalid_status, sizeof(invalid_status) - 1u,
                               "StatusNotification", "status-3");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(8u, invalid_identity, sizeof(invalid_identity) - 1u,
                               "StatusNotification", "status-4");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(9u, invalid_calendar, sizeof(invalid_calendar) - 1u,
                               "StatusNotification", "status-5");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("accepts required-only TransactionEvent facts") {
    static const uint8_t frame[] =
        "[2,\"tx-1\",\"TransactionEvent\","
        "{\"eventType\":\"Started\",\"timestamp\":\"2026-07-27T10:16:00Z\","
        "\"triggerReason\":\"Authorized\",\"seqNo\":0,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-42\"}}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(10u, frame, sizeof(frame) - 1u, "TransactionEvent", "tx-1");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects invalid TransactionEvent required facts") {
    static const uint8_t invalid_trigger[] =
        "[2,\"tx-2\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:17:00Z\","
        "\"triggerReason\":\"UnknownReason\",\"seqNo\":1,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-42\"}}]";
    static const uint8_t invalid_sequence[] =
        "[2,\"tx-3\",\"TransactionEvent\","
        "{\"eventType\":\"Ended\",\"timestamp\":\"2026-07-27T10:18:00Z\","
        "\"triggerReason\":\"EVDeparted\",\"seqNo\":-1,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-42\"}}]";
    static const uint8_t long_transaction_id[] =
        "[2,\"tx-4\",\"TransactionEvent\","
        "{\"eventType\":\"Ended\",\"timestamp\":\"2026-07-27T10:18:00Z\","
        "\"triggerReason\":\"EVDeparted\",\"seqNo\":2,"
        "\"transactionInfo\":{\"transactionId\":\"1234567890123456789012345678901234567\"}}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event;

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    event = ocpp201_core_event(11u, invalid_trigger, sizeof(invalid_trigger) - 1u,
                               "TransactionEvent", "tx-2");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(12u, invalid_sequence, sizeof(invalid_sequence) - 1u,
                               "TransactionEvent", "tx-3");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(13u, long_transaction_id, sizeof(long_transaction_id) - 1u,
                               "TransactionEvent", "tx-4");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("accepts supported TransactionEvent optional identity facts") {
    static const uint8_t frame[] =
        "[2,\"tx-5\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:19:00Z\","
        "\"triggerReason\":\"MeterValuePeriodic\",\"seqNo\":3,\"offline\":false,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-42\"},"
        "\"evse\":{\"id\":1,\"connectorId\":2},"
        "\"idToken\":{\"idToken\":\"04AABBCCDD\",\"type\":\"ISO14443\"}}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(14u, frame, sizeof(frame) - 1u, "TransactionEvent", "tx-5");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    ocpp201_core_business_close(registry, owner);
  }

  it("accepts an EVSE identity without an optional connector") {
    static const uint8_t frame[] =
        "[2,\"tx-6\",\"TransactionEvent\","
        "{\"eventType\":\"Started\",\"timestamp\":\"2026-07-27T10:20:00Z\","
        "\"triggerReason\":\"CablePluggedIn\",\"seqNo\":0,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"evse\":{\"id\":2}}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(15u, frame, sizeof(frame) - 1u, "TransactionEvent", "tx-6");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_OK);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects invalid TransactionEvent optional identities") {
    static const uint8_t invalid_evse[] =
        "[2,\"tx-7\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:21:00Z\","
        "\"triggerReason\":\"ChargingStateChanged\",\"seqNo\":4,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"evse\":{\"id\":0,\"connectorId\":1}}]";
    static const uint8_t invalid_evse_shape[] =
        "[2,\"tx-7b\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:21:01Z\","
        "\"triggerReason\":\"ChargingStateChanged\",\"seqNo\":5,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"evse\":{\"id\":1,\"connectorId\":1,\"unknown\":1}}]";
    static const uint8_t invalid_id_token[] =
        "[2,\"tx-8\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:22:00Z\","
        "\"triggerReason\":\"Authorized\",\"seqNo\":6,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"idToken\":{\"idToken\":\"04AABBCCDD\",\"type\":\"Unknown\"}}]";
    static const uint8_t empty_id_token[] =
        "[2,\"tx-8b\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:22:01Z\","
        "\"triggerReason\":\"Authorized\",\"seqNo\":7,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"idToken\":{\"idToken\":\"\",\"type\":\"Local\"}}]";
    static const uint8_t invalid_offline[] =
        "[2,\"tx-8c\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:22:02Z\","
        "\"triggerReason\":\"MeterValuePeriodic\",\"seqNo\":8,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"offline\":\"true\"}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event;

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    event = ocpp201_core_event(16u, invalid_evse, sizeof(invalid_evse) - 1u, "TransactionEvent",
                               "tx-7");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(17u, invalid_evse_shape, sizeof(invalid_evse_shape) - 1u,
                               "TransactionEvent", "tx-7b");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(18u, invalid_id_token, sizeof(invalid_id_token) - 1u,
                               "TransactionEvent", "tx-8");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(19u, empty_id_token, sizeof(empty_id_token) - 1u, "TransactionEvent",
                               "tx-8b");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(20u, invalid_offline, sizeof(invalid_offline) - 1u,
                               "TransactionEvent", "tx-8c");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects unbound TransactionEvent optional fields") {
    static const uint8_t frame[] =
        "[2,\"tx-9\",\"TransactionEvent\","
        "{\"eventType\":\"Updated\",\"timestamp\":\"2026-07-27T10:23:00Z\","
        "\"triggerReason\":\"MeterValuePeriodic\",\"seqNo\":6,"
        "\"transactionInfo\":{\"transactionId\":\"transaction-43\"},"
        "\"meterValue\":[{\"timestamp\":\"2026-07-27T10:23:00Z\","
        "\"sampledValue\":[{\"value\":1.0}]}]}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(21u, frame, sizeof(frame) - 1u, "TransactionEvent", "tx-9");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    ocpp201_core_business_close(registry, owner);
  }

  it("rejects envelope metadata disagreement and unsupported Core actions") {
    static const uint8_t mismatch[] = "[2,\"auth-1\",\"Authorize\",{}]";
    static const uint8_t unsupported[] = "[2,\"vars-1\",\"GetVariables\",{}]";
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_event_view_t event =
        ocpp201_core_event(22u, mismatch, sizeof(mismatch) - 1u, "Heartbeat", "auth-1");

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_EPROTO);
    event = ocpp201_core_event(23u, unsupported, sizeof(unsupported) - 1u, "GetVariables", "vars-1");
    check_int_eq(turbo_flow_protocol_business_consume_committed(business, &event), TURBO_ENOTSUP);
    ocpp201_core_business_close(registry, owner);
  }

  it("maps a typed reset command through the public OCPP encoder") {
    static const uint8_t request_json[] = "{\"type\":\"Immediate\"}";
    uint8_t command_payload[128];
    uint8_t frame[256];
    turbo_flow_protocol_business_registry_t *business_registry = NULL;
    turbo_flow_protocol_business_owner_t *business_owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_registry_t *protocol_registry = NULL;
    turbo_flow_protocol_owner_t *protocol_owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_business_command_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_REQUEST_INIT;
    turbo_flow_protocol_business_command_output_t output =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_OUTPUT_INIT;
    turbo_flow_protocol_command_view_t command = TURBO_FLOW_PROTOCOL_COMMAND_VIEW_INIT;
    turbo_flow_protocol_frame_output_t encoded = TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;

    check_int_eq(ocpp201_core_business_open(&business_registry, &business_owner, &business),
                 TURBO_OK);
    check_int_eq(ocpp201_core_protocol_open(&protocol_registry, &protocol_owner, &protocol), TURBO_OK);
    request.command_id = 11u;
    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.tenant = "fleet-a";
    request.device_id = "charger-1";
    request.action = "reset";
    request.correlation_id = "cmd-11";
    request.sequence = 19u;
    request.content.data = request_json;
    request.content.data_size = sizeof(request_json) - 1u;
    request.content.media_type = "application/json";
    request.content.schema_id = OCPP201_CORE_SCHEMA_ID;
    request.content.type_name = "ResetRequest";
    output.payload = command_payload;
    output.payload_capacity = sizeof(command_payload);

    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_OK);
    check_str_eq(output.device_id, "charger-1");
    check_str_eq(output.operation, "Reset");
    check_str_eq(output.correlation_id, "cmd-11");
    check_size_eq(output.payload_size, sizeof(request_json) - 1u);
    check_mem_eq(output.payload, request_json, output.payload_size);
    check_int_eq(turbo_flow_protocol_business_command_view(&output, &command), TURBO_OK);
    encoded.data = frame;
    encoded.capacity = sizeof(frame);
    check_int_eq(turbo_flow_protocol_encode(protocol, &command, &encoded), TURBO_OK);
    frame[encoded.data_size] = '\0';
    check_str_eq((const char *)frame, "[2,\"cmd-11\",\"Reset\",{\"type\":\"Immediate\"}]");

    turbo_flow_protocol_owner_destroy(protocol_owner);
    check_int_eq(turbo_flow_protocol_registry_destroy(protocol_registry), TURBO_OK);
    ocpp201_core_business_close(business_registry, business_owner);
  }

  it("maps a typed unlock command through the public OCPP encoder") {
    static const uint8_t request_json[] = "{\"evseId\":1,\"connectorId\":2}";
    uint8_t command_payload[128];
    uint8_t frame[256];
    turbo_flow_protocol_business_registry_t *business_registry = NULL;
    turbo_flow_protocol_business_owner_t *business_owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_registry_t *protocol_registry = NULL;
    turbo_flow_protocol_owner_t *protocol_owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_business_command_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_REQUEST_INIT;
    turbo_flow_protocol_business_command_output_t output =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_OUTPUT_INIT;
    turbo_flow_protocol_command_view_t command = TURBO_FLOW_PROTOCOL_COMMAND_VIEW_INIT;
    turbo_flow_protocol_frame_output_t encoded = TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;

    check_int_eq(ocpp201_core_business_open(&business_registry, &business_owner, &business),
                 TURBO_OK);
    check_int_eq(ocpp201_core_protocol_open(&protocol_registry, &protocol_owner, &protocol), TURBO_OK);
    request.command_id = 13u;
    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.tenant = "fleet-a";
    request.device_id = "charger-1";
    request.action = "unlock-connector";
    request.correlation_id = "cmd-13";
    request.sequence = 21u;
    request.content.data = request_json;
    request.content.data_size = sizeof(request_json) - 1u;
    request.content.media_type = "application/json";
    request.content.schema_id = OCPP201_CORE_SCHEMA_ID;
    request.content.type_name = "UnlockConnectorRequest";
    output.payload = command_payload;
    output.payload_capacity = sizeof(command_payload);

    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_OK);
    check_str_eq(output.device_id, "charger-1");
    check_str_eq(output.operation, "UnlockConnector");
    check_str_eq(output.correlation_id, "cmd-13");
    check_size_eq(output.payload_size, sizeof(request_json) - 1u);
    check_mem_eq(output.payload, request_json, output.payload_size);
    check_int_eq(turbo_flow_protocol_business_command_view(&output, &command), TURBO_OK);
    encoded.data = frame;
    encoded.capacity = sizeof(frame);
    check_int_eq(turbo_flow_protocol_encode(protocol, &command, &encoded), TURBO_OK);
    frame[encoded.data_size] = '\0';
    check_str_eq((const char *)frame,
                 "[2,\"cmd-13\",\"UnlockConnector\",{\"evseId\":1,\"connectorId\":2}]");

    turbo_flow_protocol_owner_destroy(protocol_owner);
    check_int_eq(turbo_flow_protocol_registry_destroy(protocol_registry), TURBO_OK);
    ocpp201_core_business_close(business_registry, business_owner);
  }

  it("rejects invalid unlock identities without partial output") {
    static const uint8_t zero_evse[] = "{\"evseId\":0,\"connectorId\":2}";
    static const uint8_t fractional_connector[] = "{\"evseId\":1,\"connectorId\":2.5}";
    uint8_t payload[128];
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_command_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_REQUEST_INIT;
    turbo_flow_protocol_business_command_output_t output =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_OUTPUT_INIT;

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    request.command_id = 14u;
    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.tenant = "fleet-a";
    request.device_id = "charger-1";
    request.action = "unlock-connector";
    request.correlation_id = "cmd-14";
    request.content.data = zero_evse;
    request.content.data_size = sizeof(zero_evse) - 1u;
    request.content.media_type = "application/json";
    request.content.schema_id = OCPP201_CORE_SCHEMA_ID;
    request.content.type_name = "UnlockConnectorRequest";
    output.payload = payload;
    output.payload_capacity = sizeof(payload);

    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EPROTO);
    check_size_eq(output.payload_size, 0u);
    request.content.data = fractional_connector;
    request.content.data_size = sizeof(fractional_connector) - 1u;
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EPROTO);
    check_size_eq(output.payload_size, 0u);
    ocpp201_core_business_close(registry, owner);
  }

  it("fails without partial output for invalid schema and small buffers") {
    static const uint8_t request_json[] = "{\"type\":\"OnIdle\"}";
    static const uint8_t invalid_enum[] = "{\"type\":\"Later\"}";
    static const uint8_t numeric_enum[] = "{\"type\":1}";
    uint8_t payload[8];
    turbo_flow_protocol_business_registry_t *registry = NULL;
    turbo_flow_protocol_business_owner_t *owner = NULL;
    turbo_flow_protocol_business_t *business = NULL;
    turbo_flow_protocol_business_command_request_t request =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_REQUEST_INIT;
    turbo_flow_protocol_business_command_output_t output =
        TURBO_FLOW_PROTOCOL_BUSINESS_COMMAND_OUTPUT_INIT;

    check_int_eq(ocpp201_core_business_open(&registry, &owner, &business), TURBO_OK);
    request.command_id = 12u;
    request.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    request.tenant = "fleet-a";
    request.device_id = "charger-1";
    request.action = "reset";
    request.correlation_id = "cmd-12";
    request.content.data = request_json;
    request.content.data_size = sizeof(request_json) - 1u;
    request.content.media_type = "application/json";
    request.content.schema_id = OCPP201_CORE_SCHEMA_ID;
    request.content.type_name = "ResetRequest";
    output.payload = payload;
    output.payload_capacity = sizeof(payload);
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EMSGSIZE);
    check_size_eq(output.payload_size, 0u);
    output.payload_capacity = sizeof(payload);
    request.content.schema_id = "OtherSchema";
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EINVAL);
    check_size_eq(output.payload_size, 0u);
    request.content.schema_id = OCPP201_CORE_SCHEMA_ID;
    request.content.data = invalid_enum;
    request.content.data_size = sizeof(invalid_enum) - 1u;
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EPROTO);
    check_size_eq(output.payload_size, 0u);
    request.content.data = numeric_enum;
    request.content.data_size = sizeof(numeric_enum) - 1u;
    check_int_eq(turbo_flow_protocol_business_prepare_command(business, &request, &output),
                 TURBO_EPROTO);
    check_size_eq(output.payload_size, 0u);
    ocpp201_core_business_close(registry, owner);
  }
}
