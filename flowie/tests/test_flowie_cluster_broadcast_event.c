#include "flowie_cluster_broadcast_event_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

static int flowie_cluster_broadcast_test_publish(tstr_t *out) {
  static const uint8_t client_id[] = "publisher-a";
  static const uint8_t publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                    0x00u, 0x07u, 0x00u, 'o',   'k'};
  uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
  edge_boot[0] = 0x11u;
  return flowie_cluster_publish_event_encode(
      FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 3u, 4u, 5u, 6u, 1000u,
      tstr_v_from_cstr("edge-a"), edge_boot,
      (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
      (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, out);
}

spec("flowie cluster broadcast event envelope") {
  it("carries stable PostgreSQL identity independently of Redis metadata") {
    flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    flowie_cluster_broadcast_event_view_t decoded = FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
    tstr_t encoded = NULL;
    event.command_id[0] = 0x21u;
    event.event_index = 2u;
    event.shard_id = 7u;
    event.event_owner_epoch = 11u;
    event.fact_revision = 0u;
    event.event_type = FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE;
    event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
    event.record_key = tstr_new_len("publisher-a", sizeof("publisher-a") - 1u);
    check_not_null(event.record_key);
    check_int_eq(flowie_cluster_broadcast_test_publish(&event.payload), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_event_encode(&event, 2048u, &encoded), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_event_decode(encoded, tstr_len(encoded), 2048u, &decoded),
                 TURBO_OK);
    check_int_eq(decoded.command_id[0], 0x21u);
    check_int_eq(decoded.event_index, 2u);
    check_int_eq(decoded.source_shard_id, 7u);
    check_uint_eq(decoded.source_owner_epoch, 11u);
    check_uint_eq(decoded.fact_revision, 0u);
    check_uint_eq(decoded.publish.session_id, 5u);
    check_mem_eq(decoded.publish.publish.client_id.data, "publisher-a", 11u);
    tstr_free(encoded);
    tstr_free(event.record_key);
    tstr_free(event.payload);
  }

  it("rejects mismatched source identity and reserved envelope fields") {
    flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    flowie_cluster_broadcast_event_view_t decoded = FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
    tstr_t encoded = NULL;
    event.command_id[0] = 0x31u;
    event.shard_id = 8u;
    event.event_owner_epoch = 12u;
    event.event_type = FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE;
    event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
    event.record_key = tstr_new_len("another-client", sizeof("another-client") - 1u);
    check_not_null(event.record_key);
    check_int_eq(flowie_cluster_broadcast_test_publish(&event.payload), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_event_encode(&event, 2048u, &encoded), TURBO_EPROTO);
    check_null(encoded);
    tstr_freep(&event.record_key);
    event.record_key = tstr_new_len("publisher-a", sizeof("publisher-a") - 1u);
    check_not_null(event.record_key);
    check_int_eq(flowie_cluster_broadcast_event_encode(&event, 2048u, &encoded), TURBO_OK);
    encoded[60] = 1;
    check_int_eq(flowie_cluster_broadcast_event_decode(encoded, tstr_len(encoded), 2048u, &decoded),
                 TURBO_EPROTO);
    tstr_free(encoded);
    tstr_free(event.record_key);
    tstr_free(event.payload);
  }

  it("derives a stable dedupe digest only from a valid TFBE envelope") {
    flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
    uint8_t first[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
    uint8_t replay[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
    uint8_t changed[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
    tstr_t encoded = NULL;
    event.command_id[0] = 0x41u;
    event.shard_id = 2u;
    event.event_owner_epoch = 13u;
    event.event_type = FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE;
    event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
    event.record_key = tstr_new_len("publisher-a", sizeof("publisher-a") - 1u);
    check_not_null(event.record_key);
    check_int_eq(flowie_cluster_broadcast_test_publish(&event.payload), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_event_encode(&event, 2048u, &encoded), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_event_digest(encoded, tstr_len(encoded), 2048u, first),
                 TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_event_digest(encoded, tstr_len(encoded), 2048u, replay),
                 TURBO_OK);
    check_mem_eq(first, replay, sizeof(first));
    encoded[55] = 1u;
    check_int_eq(flowie_cluster_broadcast_event_digest(encoded, tstr_len(encoded), 2048u, changed),
                 TURBO_OK);
    check_mem_ne(first, changed, sizeof(first));
    encoded[60] = 1u;
    check_int_eq(flowie_cluster_broadcast_event_digest(encoded, tstr_len(encoded), 2048u, changed),
                 TURBO_EPROTO);
    check_mem_eq(changed, (uint8_t[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE]){0}, sizeof(changed));
    tstr_free(encoded);
    tstr_free(event.record_key);
    tstr_free(event.payload);
  }

  it("derives stable distinct target transaction command IDs") {
    uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE] = {1u};
    uint8_t first[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
    uint8_t replay[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
    uint8_t another_session[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
    uint8_t another_shard[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
    check_int_eq(flowie_cluster_broadcast_target_command_id(digest, 3u, 41u, first), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_command_id(digest, 3u, 41u, replay), TURBO_OK);
    check_mem_eq(first, replay, sizeof(first));
    check_int_eq(flowie_cluster_broadcast_target_command_id(digest, 3u, 42u, another_session),
                 TURBO_OK);
    check_mem_ne(first, another_session, sizeof(first));
    check_int_eq(flowie_cluster_broadcast_target_command_id(digest, 4u, 41u, another_shard),
                 TURBO_OK);
    check_mem_ne(first, another_shard, sizeof(first));
    memset(digest, 0, sizeof(digest));
    check_int_eq(flowie_cluster_broadcast_target_command_id(digest, 3u, 41u, replay),
                 TURBO_EINVAL);
    check_mem_eq(replay, (uint8_t[FLOWIE_CLUSTER_COMMAND_ID_SIZE]){0}, sizeof(replay));
  }

  it("builds a shard-complete marker without a target mutation") {
    flowie_cluster_broadcast_event_view_t source = FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_event_dedupe_t dedupe = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
    flowie_cluster_pgsql_fact_command_t command = FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
    uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};
    uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE] = {2u};
    uint8_t session_command[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
    source.command_id[0] = 3u;
    source.event_index = 4u;
    source.source_shard_id = 5u;
    source.source_owner_epoch = 6u;
    source.fact_revision = 7u;
    check_int_eq(flowie_cluster_owner_token_init(&owner, 8u, 9u, "owner-a", 7u, boot),
                 TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_shard_ack_command(&owner, &source, digest, &dedupe,
                                                            &command),
                 TURBO_OK);
    check_size_eq(command.mutation_count, 0u);
    check_null(command.mutations);
    check(command.dedupe == &dedupe);
    check_uint_eq(dedupe.target_session_id, 0u);
    check_uint_eq(dedupe.source_shard_id, 5u);
    check_uint_eq(dedupe.source_owner_epoch, 6u);
    check_mem_eq(dedupe.event_digest, digest, sizeof(digest));
    check_int_eq(flowie_cluster_broadcast_target_command_id(digest, owner.shard_id, 41u,
                                                            session_command),
                 TURBO_OK);
    check_mem_ne(command.command_id, session_command, sizeof(session_command));
  }
}
