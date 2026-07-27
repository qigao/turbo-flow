#include "ocpp201_core.h"
#include "tinytest.h"

#include <cstring>

spec("OCPP 2.0.1 Core generated DataBind ABI") {
  it("publishes the additive schema revision") { check_int_eq(Ocpp201Core_SCHEMA_VERSION, 5); }

  it("publishes the complete TransactionEvent required enum sets") {
    check_size_eq(TransactionEventType_count(), 3u);
    check_size_eq(TriggerReason_count(), 21u);
    check_size_eq(IdTokenType_count(), 8u);
    check_str_eq(TransactionEventType_to_string(TransactionEventType_Updated), "Updated");
    check_str_eq(TriggerReason_to_string(TriggerReason_ResetCommand), "ResetCommand");
    check_str_eq(IdTokenType_to_string(IdTokenType_NoAuthorization), "NoAuthorization");
  }

  it("binds BootNotification typed units into owning C fields") {
    static const char station_json[] = "{\"model\":\"Model-A\",\"vendorName\":\"Vendor-A\"}";
    static const char reason_json[] = "{\"reason\":\"PowerUp\"}";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = nullptr;
    ChargingStation_t station;
    BootNotificationReason_t reason;

    check_int_eq(Ocpp201Core_codec_create(&codec, &error), DATA_BIND_OK);
    ChargingStation_init(&station);
    BootNotificationReason_init(&reason);
    check_int_eq(
        ChargingStation_from_json(codec, &station, station_json, sizeof(station_json) - 1u, &error),
        DATA_BIND_OK);
    check_int_eq(BootNotificationReason_from_json(codec, &reason, reason_json,
                                                  sizeof(reason_json) - 1u, &error),
                 DATA_BIND_OK);
    check_size_eq(tstr_len(station.model), sizeof("Model-A") - 1u);
    check_size_eq(tstr_len(station.vendorName), sizeof("Vendor-A") - 1u);
    check_int_eq(reason.reason, BootReason_PowerUp);

    BootNotificationReason_clear(&reason);
    ChargingStation_clear(&station);
    data_bind_free(codec);
  }

  it("binds StatusNotification into typed state facts") {
    static const char json[] =
        "{\"timestamp\":\"2026-07-27T10:15:30Z\",\"connectorStatus\":\"Faulted\","
        "\"evseId\":3,\"connectorId\":2}";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = nullptr;
    StatusNotificationRequest_t notification;

    check_int_eq(Ocpp201Core_codec_create(&codec, &error), DATA_BIND_OK);
    StatusNotificationRequest_init(&notification);
    check_int_eq(
        StatusNotificationRequest_from_json(codec, &notification, json, sizeof(json) - 1u, &error),
        DATA_BIND_OK);
    check_size_eq(tstr_len(notification.timestamp), sizeof("2026-07-27T10:15:30Z") - 1u);
    check_int_eq(notification.connectorStatus, ConnectorStatus_Faulted);
    check_int_eq(notification.evseId, 3);
    check_int_eq(notification.connectorId, 2);

    StatusNotificationRequest_clear(&notification);
    data_bind_free(codec);
  }

  it("binds TransactionEvent required units without crossing the DLL ABI") {
    static const char event_json[] =
        "{\"eventType\":\"Started\",\"timestamp\":\"2026-07-27T10:16:00Z\","
        "\"triggerReason\":\"Authorized\",\"seqNo\":0}";
    static const char transaction_json[] = "{\"transactionId\":\"transaction-42\"}";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = nullptr;
    TransactionEventRequired_t event;
    TransactionInfoRequired_t transaction;

    check_int_eq(Ocpp201Core_codec_create(&codec, &error), DATA_BIND_OK);
    TransactionEventRequired_init(&event);
    TransactionInfoRequired_init(&transaction);
    check_int_eq(TransactionEventRequired_from_json(codec, &event, event_json,
                                                    sizeof(event_json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(TransactionInfoRequired_from_json(codec, &transaction, transaction_json,
                                                   sizeof(transaction_json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(event.eventType, TransactionEventType_Started);
    check_int_eq(event.triggerReason, TriggerReason_Authorized);
    check_int_eq(event.seqNo, 0);
    check_size_eq(tstr_len(event.timestamp), sizeof("2026-07-27T10:16:00Z") - 1u);
    check_size_eq(tstr_len(transaction.transactionId), sizeof("transaction-42") - 1u);

    TransactionInfoRequired_clear(&transaction);
    TransactionEventRequired_clear(&event);
    data_bind_free(codec);
  }

  it("binds supported TransactionEvent optional facts with generated presence") {
    static const char event_json[] =
        "{\"eventType\":\"Updated\",\"triggerReason\":\"MeterValuePeriodic\",\"seqNo\":3,"
        "\"offline\":false,\"timestamp\":\"2026-07-27T10:19:00Z\"}";
    static const char event_without_offline_json[] =
        "{\"eventType\":\"Started\",\"triggerReason\":\"CablePluggedIn\",\"seqNo\":0,"
        "\"timestamp\":\"2026-07-27T10:20:00Z\"}";
    static const char evse_json[] = "{\"id\":1,\"connectorId\":2}";
    static const char evse_without_connector_json[] = "{\"id\":2}";
    static const char id_token_json[] = "{\"idToken\":\"04AABBCCDD\",\"type\":\"ISO14443\"}";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = nullptr;
    TransactionEventFacts_t event;
    TransactionEventFacts_t event_without_offline;
    TransactionEvseFacts_t evse;
    TransactionEvseFacts_t evse_without_connector;
    TransactionIdToken_t id_token;

    check_int_eq(Ocpp201Core_codec_create(&codec, &error), DATA_BIND_OK);
    TransactionEventFacts_init(&event);
    TransactionEventFacts_init(&event_without_offline);
    TransactionEvseFacts_init(&evse);
    TransactionEvseFacts_init(&evse_without_connector);
    TransactionIdToken_init(&id_token);
    check_int_eq(TransactionEventFacts_from_json(codec, &event, event_json,
                                                 sizeof(event_json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(TransactionEventFacts_from_json(
                     codec, &event_without_offline, event_without_offline_json,
                     sizeof(event_without_offline_json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(TransactionEvseFacts_from_json(codec, &evse, evse_json,
                                                sizeof(evse_json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(TransactionEvseFacts_from_json(
                     codec, &evse_without_connector, evse_without_connector_json,
                     sizeof(evse_without_connector_json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(TransactionIdToken_from_json(codec, &id_token, id_token_json,
                                              sizeof(id_token_json) - 1u, &error),
                 DATA_BIND_OK);
    check_uint_eq(event._presence[0], 1u << TransactionEventFacts_OPTIONAL_offline);
    check_false(event.offline);
    check_uint_eq(event_without_offline._presence[0], 0u);
    check_int_eq(evse.id, 1);
    check_uint_eq(evse._presence[0], 1u << TransactionEvseFacts_OPTIONAL_connectorId);
    check_int_eq(evse.connectorId, 2);
    check_int_eq(evse_without_connector.id, 2);
    check_uint_eq(evse_without_connector._presence[0], 0u);
    check_size_eq(tstr_len(id_token.idToken), sizeof("04AABBCCDD") - 1u);
    check_int_eq(id_token.type, IdTokenType_ISO14443);

    TransactionIdToken_clear(&id_token);
    TransactionEvseFacts_clear(&evse_without_connector);
    TransactionEvseFacts_clear(&evse);
    TransactionEventFacts_clear(&event_without_offline);
    TransactionEventFacts_clear(&event);
    data_bind_free(codec);
  }

  it("round-trips ResetRequest from a C++ shared-library consumer") {
    static const char json[] = "{\"type\":\"Immediate\"}";
    static const char encoded_json[] = "{\"type\":1}";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = nullptr;
    ResetRequest_t request;
    char *encoded = nullptr;
    size_t encoded_size = 0u;

    check_int_eq(data_bind_abi_version(), DATA_BIND_ABI_VERSION);
    check_int_eq(Ocpp201Core_codec_create(&codec, &error), DATA_BIND_OK);
    ResetRequest_init(&request);
    check_int_eq(ResetRequest_from_json(codec, &request, json, sizeof(json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(ResetRequest_to_json(codec, &request, &encoded, &encoded_size, &error),
                 DATA_BIND_OK);
    check_size_eq(encoded_size, sizeof(encoded_json) - 1u);
    check_mem_eq(encoded, encoded_json, encoded_size);

    tbe_typed_serialized_free(encoded);
    ResetRequest_clear(&request);
    data_bind_free(codec);
  }

  it("round-trips UnlockConnectorRequest from a C++ shared-library consumer") {
    static const char json[] = "{\"evseId\":1,\"connectorId\":2}";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = nullptr;
    UnlockConnectorRequest_t request;
    char *encoded = nullptr;
    size_t encoded_size = 0u;

    check_int_eq(Ocpp201Core_codec_create(&codec, &error), DATA_BIND_OK);
    UnlockConnectorRequest_init(&request);
    check_int_eq(UnlockConnectorRequest_from_json(codec, &request, json, sizeof(json) - 1u, &error),
                 DATA_BIND_OK);
    check_int_eq(request.evseId, 1);
    check_int_eq(request.connectorId, 2);
    check_int_eq(UnlockConnectorRequest_to_json(codec, &request, &encoded, &encoded_size, &error),
                 DATA_BIND_OK);
    check_size_eq(encoded_size, sizeof(json) - 1u);
    check_mem_eq(encoded, json, encoded_size);

    tbe_typed_serialized_free(encoded);
    UnlockConnectorRequest_clear(&request);
    data_bind_free(codec);
  }
}
