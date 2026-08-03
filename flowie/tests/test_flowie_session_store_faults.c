#include "flowie.h"
#include "flowie_session_internal.h"
#include "flowie_test_socket.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_STORE_TEST_WAIT_STEPS 2000u
#define FLOWIE_STORE_TEST_MAX_VALUE_SIZE (1024u * 1024u)
#define FLOWIE_STORE_TEST_MAX_KEY_SIZE 65538u
#define FLOWIE_STORE_TEST_ENDPOINT_OWNER_RECORD_TYPE 10u

static const uint8_t persistent_connect[] = {
    0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',  0x05u, 0x00u,
    0x00u, 0x3cu, 0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u,
    'd',   'u',   'r'};

typedef struct flowie_test_record_store_s {
  atomic_int fail_commits;
  atomic_int scan_status;
  atomic_int lose_commit_reply;
  atomic_int present;
  atomic_size_t commit_calls;
  atomic_size_t scan_calls;
  atomic_uint_fast64_t last_expected_revision;
  atomic_uint_fast64_t last_next_revision;
  uint8_t *key;
  size_t key_size;
  uint64_t revision;
  uint8_t *value;
  size_t value_size;
} flowie_test_record_store_t;

typedef struct flowie_test_record_store_pair_s {
  flowie_test_record_store_t session;
  flowie_test_record_store_t retained;
  turbo_flow_record_store_t session_store;
  turbo_flow_record_store_t retained_store;
  atomic_int fail_retained_commits;
  atomic_int lose_retained_reply;
} flowie_test_record_store_pair_t;

static int flowie_test_record_scan(void *ctx, turbo_flow_record_visit_fn visit, void *visit_ctx) {
  flowie_test_record_store_t *store = (flowie_test_record_store_t *)ctx;
  turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
  if (!store || !visit) return TURBO_EINVAL;
  atomic_fetch_add_explicit(&store->scan_calls, 1u, memory_order_relaxed);
  if (atomic_load_explicit(&store->scan_status, memory_order_acquire) != TURBO_OK)
    return atomic_load_explicit(&store->scan_status, memory_order_relaxed);
  if (!atomic_load_explicit(&store->present, memory_order_acquire)) return TURBO_OK;
  record.key = store->key;
  record.key_size = store->key_size;
  record.revision = store->revision;
  record.value = store->value;
  record.value_size = store->value_size;
  return visit(visit_ctx, &record);
}

static int flowie_test_record_commit(void *ctx,
                                     const turbo_flow_record_mutation_t *mutations,
                                     size_t mutation_count) {
  flowie_test_record_store_t *store = (flowie_test_record_store_t *)ctx;
  const turbo_flow_record_mutation_t *mutation;
  uint8_t *next_key = NULL;
  uint8_t *next_value = NULL;
  uint64_t current_revision;
  if (!store || !mutations || mutation_count != 1u) return TURBO_EINVAL;
  atomic_fetch_add_explicit(&store->commit_calls, 1u, memory_order_relaxed);
  atomic_store_explicit(&store->last_expected_revision, mutations[0].expected_revision,
                        memory_order_relaxed);
  atomic_store_explicit(&store->last_next_revision, mutations[0].next_revision,
                        memory_order_relaxed);
  if (atomic_load_explicit(&store->fail_commits, memory_order_acquire)) return TURBO_EIO;
  mutation = &mutations[0];
  if (mutation->size < sizeof(*mutation) || !mutation->key || mutation->key_size == 0u ||
      mutation->key_size > FLOWIE_STORE_TEST_MAX_KEY_SIZE)
    return TURBO_EINVAL;
  current_revision = atomic_load_explicit(&store->present, memory_order_acquire)
                         ? store->revision
                         : TURBO_FLOW_RECORD_REVISION_ABSENT;
  if (mutation->expected_revision != current_revision) return TURBO_EBUSY;
  if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
    if (mutation->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || mutation->value ||
        mutation->value_size != 0u)
      return TURBO_EINVAL;
    atomic_store_explicit(&store->present, 0, memory_order_release);
    free(store->key);
    free(store->value);
    store->key = NULL;
    store->key_size = 0u;
    store->revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
    store->value = NULL;
    store->value_size = 0u;
    return TURBO_OK;
  }
  if (mutation->kind != TURBO_FLOW_RECORD_PUT || !mutation->value ||
      mutation->value_size == 0u ||
      mutation->value_size > FLOWIE_STORE_TEST_MAX_VALUE_SIZE ||
      mutation->next_revision <= mutation->expected_revision)
    return TURBO_EINVAL;
  next_key = (uint8_t *)malloc(mutation->key_size);
  next_value = (uint8_t *)malloc(mutation->value_size);
  if (!next_key || !next_value) {
    free(next_key);
    free(next_value);
    return TURBO_ENOMEM;
  }
  memcpy(next_key, mutation->key, mutation->key_size);
  memcpy(next_value, mutation->value, mutation->value_size);
  free(store->key);
  free(store->value);
  store->key = next_key;
  store->key_size = mutation->key_size;
  store->revision = mutation->next_revision;
  store->value = next_value;
  store->value_size = mutation->value_size;
  atomic_store_explicit(&store->present, 1, memory_order_release);
  return atomic_exchange_explicit(&store->lose_commit_reply, 0, memory_order_acq_rel) ? TURBO_EIO
                                                                                     : TURBO_OK;
}

static void flowie_test_record_store_init(flowie_test_record_store_t *fixture,
                                          turbo_flow_record_store_t *store) {
  memset(fixture, 0, sizeof(*fixture));
  atomic_init(&fixture->fail_commits, 0);
  atomic_init(&fixture->scan_status, TURBO_OK);
  atomic_init(&fixture->lose_commit_reply, 0);
  atomic_init(&fixture->present, 0);
  atomic_init(&fixture->commit_calls, 0u);
  atomic_init(&fixture->scan_calls, 0u);
  atomic_init(&fixture->last_expected_revision, TURBO_FLOW_RECORD_REVISION_ABSENT);
  atomic_init(&fixture->last_next_revision, TURBO_FLOW_RECORD_REVISION_ABSENT);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  store->capabilities =
      TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  store->max_key_size = FLOWIE_STORE_TEST_MAX_KEY_SIZE;
  store->max_value_size = FLOWIE_STORE_TEST_MAX_VALUE_SIZE;
  store->max_batch_size = 1u;
  store->max_records = 8u;
  store->ctx = fixture;
  store->scan = flowie_test_record_scan;
  store->commit = flowie_test_record_commit;
}

static void flowie_test_record_store_destroy(flowie_test_record_store_t *store) {
  if (!store) return;
  free(store->key);
  free(store->value);
  store->key = NULL;
  store->value = NULL;
}

static int flowie_test_record_pair_is_retained(const turbo_flow_record_mutation_t *mutation) {
  static const uint8_t retained_prefix[] = {0u, 'R', 1u};
  return mutation && mutation->key && mutation->key_size > sizeof(retained_prefix) &&
         memcmp(mutation->key, retained_prefix, sizeof(retained_prefix)) == 0;
}

static int flowie_test_record_pair_scan(void *ctx, turbo_flow_record_visit_fn visit,
                                        void *visit_ctx) {
  flowie_test_record_store_pair_t *pair = (flowie_test_record_store_pair_t *)ctx;
  int rc;
  if (!pair || !visit) return TURBO_EINVAL;
  rc = flowie_test_record_scan(&pair->session, visit, visit_ctx);
  return rc == TURBO_OK ? flowie_test_record_scan(&pair->retained, visit, visit_ctx) : rc;
}

static int flowie_test_record_pair_commit(void *ctx,
                                          const turbo_flow_record_mutation_t *mutations,
                                          size_t mutation_count) {
  flowie_test_record_store_pair_t *pair = (flowie_test_record_store_pair_t *)ctx;
  if (!pair || !mutations || mutation_count != 1u) return TURBO_EINVAL;
  if (!flowie_test_record_pair_is_retained(&mutations[0]))
    return flowie_test_record_commit(&pair->session, mutations, mutation_count);
  atomic_store_explicit(&pair->retained.fail_commits,
                        atomic_load_explicit(&pair->fail_retained_commits, memory_order_acquire),
                        memory_order_release);
  atomic_store_explicit(&pair->retained.lose_commit_reply,
                        atomic_exchange_explicit(&pair->lose_retained_reply, 0,
                                                 memory_order_acq_rel),
                        memory_order_release);
  return flowie_test_record_commit(&pair->retained, mutations, mutation_count);
}

static void flowie_test_record_store_pair_init(flowie_test_record_store_pair_t *pair,
                                               turbo_flow_record_store_t *store) {
  memset(pair, 0, sizeof(*pair));
  flowie_test_record_store_init(&pair->session, &pair->session_store);
  flowie_test_record_store_init(&pair->retained, &pair->retained_store);
  atomic_init(&pair->fail_retained_commits, 0);
  atomic_init(&pair->lose_retained_reply, 0);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  store->capabilities = TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  store->max_key_size = FLOWIE_STORE_TEST_MAX_KEY_SIZE;
  store->max_value_size = FLOWIE_STORE_TEST_MAX_VALUE_SIZE;
  store->max_batch_size = 1u;
  store->max_records = 8u;
  store->ctx = pair;
  store->scan = flowie_test_record_pair_scan;
  store->commit = flowie_test_record_pair_commit;
}

static void flowie_test_record_store_pair_destroy(flowie_test_record_store_pair_t *pair) {
  if (!pair) return;
  flowie_test_record_store_destroy(&pair->session);
  flowie_test_record_store_destroy(&pair->retained);
}

static int flowie_test_record_put(flowie_test_record_store_t *store, const uint8_t *key,
                                  size_t key_size, uint64_t expected_revision,
                                  uint64_t next_revision, const uint8_t *value,
                                  size_t value_size) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  mutation.kind = TURBO_FLOW_RECORD_PUT;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.expected_revision = expected_revision;
  mutation.next_revision = next_revision;
  mutation.value = value;
  mutation.value_size = value_size;
  return flowie_test_record_commit(store, &mutation, 1u);
}

static int flowie_test_record_delete(flowie_test_record_store_t *store, const uint8_t *key,
                                     size_t key_size, uint64_t expected_revision) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.expected_revision = expected_revision;
  mutation.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  return flowie_test_record_commit(store, &mutation, 1u);
}

typedef struct flowie_test_expiry_cleanup_s {
  flowie_test_record_store_t *store;
  const uint8_t *key;
  size_t key_size;
  uint64_t expected_revision;
  atomic_int ready;
  atomic_int proceed;
  atomic_int status;
} flowie_test_expiry_cleanup_t;

static void flowie_test_expiry_cleanup_thread(void *arg) {
  flowie_test_expiry_cleanup_t *cleanup = (flowie_test_expiry_cleanup_t *)arg;
  if (!cleanup) return;
  atomic_store_explicit(&cleanup->ready, 1, memory_order_release);
  while (!atomic_load_explicit(&cleanup->proceed, memory_order_acquire)) turbo_thread_yield();
  atomic_store_explicit(
      &cleanup->status,
      flowie_test_record_delete(cleanup->store, cleanup->key, cleanup->key_size,
                                cleanup->expected_revision),
      memory_order_release);
}

static int flowie_test_wait_commits(flowie_test_record_store_t *store, size_t expected) {
  for (size_t i = 0u; i < FLOWIE_STORE_TEST_WAIT_STEPS; ++i) {
    if (atomic_load_explicit(&store->commit_calls, memory_order_acquire) >= expected)
      return TURBO_OK;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static turbo_flow_t *flowie_test_persistent_flow_with_send_hwm(
    unsigned short port, turbo_flow_record_store_t *store, size_t send_hwm_bytes) {
  static const char graph[] = "source mqtt_in adapter mqtt.endpoint\n"
                              "stage mqtt_fanout adapter mqtt.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
  struct {
    flowie_endpoint_bindings_t v1;
    const void *unknown_future_binding;
  } bindings = {FLOWIE_ENDPOINT_BINDINGS_INIT, NULL};
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_packet_size = 4096u;
  config.max_connections = 4u;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = send_hwm_bytes;
  config.manage_sessions = 1;
  config.max_sessions = 4u;
  config.max_retained_messages = 4u;
  config.max_subscriptions_per_session = 8u;
  config.max_inflight_per_session = 8u;
  persistence.store_channel = "mqtt.sessions";
  persistence.store = store;
  bindings.v1.size = sizeof(bindings);
  bindings.v1.persistence = &persistence;
  if (flowie_register_bound_endpoint(flow, "mqtt.endpoint", &config, &bindings.v1) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_test_persistent_flow(unsigned short port,
                                                 turbo_flow_record_store_t *store) {
  return flowie_test_persistent_flow_with_send_hwm(port, store, 0u);
}

static int flowie_test_restore_will_state(const flowie_test_record_store_t *store,
                                          const char *client_id, int *has_will,
                                          int *will_pending) {
  flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_owner_t *owner = NULL;
  size_t offset = 0u;
  uint8_t previous_type = 0u;
  int rc = TURBO_EPROTO;
  if (has_will) *has_will = 0;
  if (will_pending) *will_pending = 0;
  if (!store || !client_id || !has_will || !will_pending ||
      !atomic_load_explicit(&store->present, memory_order_acquire))
    return TURBO_EINVAL;
  config.owner_instance_id = 1u;
  config.session_id = 1u;
  config.max_subscriptions = 8u;
  config.max_inflight = 8u;
  while (offset < store->value_size) {
    turbo_ltv_message_t *message = NULL;
    uint32_t payload_size = 0u;
    size_t header_size = 0u;
    size_t wire_size;
    uint8_t type;
    rc = turbo_ltv_peek_size(store->value + offset, store->value_size - offset, &payload_size,
                             &header_size);
    if (rc != TURBO_OK || payload_size == 0u || header_size > store->value_size - offset ||
        payload_size > store->value_size - offset - header_size) {
      rc = TURBO_EPROTO;
      break;
    }
    wire_size = header_size + payload_size;
    rc = turbo_parse_ltv(store->value + offset, wire_size, &message);
    if (rc != TURBO_OK || !message) {
      turbo_free_ltv(&message);
      rc = TURBO_EPROTO;
      break;
    }
    type = turbo_ltv_type(message);
    if (type <= previous_type) {
      turbo_free_ltv(&message);
      rc = TURBO_EPROTO;
      break;
    }
    previous_type = type;
    if (type == FLOWIE_STORE_TEST_ENDPOINT_OWNER_RECORD_TYPE) {
      if (offset + wire_size != store->value_size) {
        turbo_free_ltv(&message);
        rc = TURBO_EPROTO;
        break;
      }
      rc = flowie_session_owner_record_restore(
          &config, (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)},
          store->revision, turbo_ltv_value(message), turbo_ltv_value_len(message), &owner);
      turbo_free_ltv(&message);
      break;
    }
    turbo_free_ltv(&message);
    offset += wire_size;
  }
  if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc == TURBO_OK) {
    *has_will = snapshot.has_will != 0u;
    *will_pending = snapshot.will_pending != 0u;
  }
  flowie_session_owner_destroy(owner);
  return rc;
}

static int flowie_test_lost_commit_recovery(unsigned short first_port,
                                            unsigned short second_port) {
  flowie_test_record_store_t fixture;
  turbo_flow_record_store_t store;
  turbo_flow_t *first = NULL;
  turbo_flow_t *restored = NULL;
  flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
  flowie_test_socket_t resumed = FLOWIE_TEST_INVALID_SOCKET;
  int first_started = 0;
  int restored_started = 0;
  int rc = TURBO_OK;

  flowie_test_record_store_init(&fixture, &store);
  atomic_store_explicit(&fixture.lose_commit_reply, 1, memory_order_release);
  first = flowie_test_persistent_flow(first_port, &store);
  if (!first) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  rc = turbo_flow_start(first);
  if (rc != TURBO_OK) goto cleanup;
  first_started = 1;
  client = flowie_test_connect(first_port);
  if (client == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ENOTCONN;
    goto cleanup;
  }
  rc = flowie_test_send(client, persistent_connect, sizeof(persistent_connect));
  if (rc != TURBO_OK) goto cleanup;
  {
    uint8_t connack[5];
    if (flowie_test_recv_exact(client, connack, sizeof(connack)) == TURBO_OK) {
      rc = TURBO_EPROTO;
      goto cleanup;
    }
  }
  rc = flowie_test_wait_commits(&fixture, 1u);
  if (rc != TURBO_OK) goto cleanup;
  if (!atomic_load_explicit(&fixture.present, memory_order_acquire)) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  flowie_test_socket_close(client);
  client = FLOWIE_TEST_INVALID_SOCKET;
  rc = turbo_flow_stop(first);
  first_started = 0;
  if (rc != TURBO_OK) goto cleanup;
  turbo_flow_destroy(first);
  first = NULL;

  restored = flowie_test_persistent_flow(second_port, &store);
  if (!restored) {
    rc = TURBO_ENOMEM;
    goto cleanup;
  }
  rc = turbo_flow_start(restored);
  if (rc != TURBO_OK) goto cleanup;
  restored_started = 1;
  resumed = flowie_test_connect(second_port);
  if (resumed == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ENOTCONN;
    goto cleanup;
  }
  rc = flowie_test_send(resumed, persistent_connect, sizeof(persistent_connect));
  if (rc == TURBO_OK) rc = flowie_test_recv_mqtt5_connack(resumed, 1u, 8u, 4096u);

cleanup:
  if (resumed != FLOWIE_TEST_INVALID_SOCKET) flowie_test_socket_close(resumed);
  if (client != FLOWIE_TEST_INVALID_SOCKET) flowie_test_socket_close(client);
  if (restored) {
    if (restored_started) {
      int stop_rc = turbo_flow_stop(restored);
      if (rc == TURBO_OK) rc = stop_rc;
    }
    turbo_flow_destroy(restored);
  }
  if (first) {
    if (first_started) {
      int stop_rc = turbo_flow_stop(first);
      if (rc == TURBO_OK) rc = stop_rc;
    }
    turbo_flow_destroy(first);
  }
  flowie_test_record_store_destroy(&fixture);
  return rc;
}

spec("Flowie durable session store failure boundaries") {
  it("MQTT-STORE-001 does not expose a failed CONNECT commit and retries from absent") {
    uint8_t received[5];
    unsigned short port = flowie_test_port();
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    turbo_flow_t *flow;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t retry = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_record_store_init(&fixture, &store);
    atomic_store_explicit(&fixture.fail_commits, 1, memory_order_release);
    flow = flowie_test_persistent_flow(port, &store);
    check_int_gt(port, 0);
    check_not_null(flow);
    if (!flow) {
      flowie_test_record_store_destroy(&fixture);
      return;
    }
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_ne(flowie_test_recv_exact(client, received, sizeof(received)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture, 1u), TURBO_OK);
    check_false(atomic_load_explicit(&fixture.present, memory_order_acquire));
    flowie_test_socket_close(client);

    atomic_store_explicit(&fixture.fail_commits, 0, memory_order_release);
    retry = flowie_test_connect(port);
    check_true(retry != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(retry, persistent_connect, sizeof(persistent_connect)), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(retry, 0u, 8u, 4096u), TURBO_OK);
    check_size_eq(atomic_load_explicit(&fixture.commit_calls, memory_order_acquire), 2u);
    check_true(atomic_load_explicit(&fixture.present, memory_order_acquire));
    check_uint_eq(atomic_load_explicit(&fixture.last_expected_revision, memory_order_acquire),
                  TURBO_FLOW_RECORD_REVISION_ABSENT);
    check_uint_eq(fixture.revision,
                  atomic_load_explicit(&fixture.last_next_revision, memory_order_acquire));
    check_true(fixture.revision > TURBO_FLOW_RECORD_REVISION_ABSENT);

    flowie_test_socket_close(retry);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-002 restores a commit after the client closes before consuming CONNACK") {
    unsigned short first_port = flowie_test_port();
    unsigned short second_port = flowie_test_port();
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    turbo_flow_t *first;
    turbo_flow_t *restored;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t resumed = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_record_store_init(&fixture, &store);
    first = flowie_test_persistent_flow(first_port, &store);
    check_int_gt(first_port, 0);
    check_int_gt(second_port, 0);
    check_not_null(first);
    if (!first) {
      flowie_test_record_store_destroy(&fixture);
      return;
    }
    check_int_eq(turbo_flow_start(first), TURBO_OK);
    client = flowie_test_connect(first_port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_test_wait_commits(&fixture, 1u), TURBO_OK);
    check_int_eq(turbo_flow_stop(first), TURBO_OK);
    turbo_flow_destroy(first);
    check_true(atomic_load_explicit(&fixture.present, memory_order_acquire));
    check_true(fixture.revision > 0u);

    restored = flowie_test_persistent_flow(second_port, &store);
    check_not_null(restored);
    check_size_eq(atomic_load_explicit(&fixture.scan_calls, memory_order_acquire), 2u);
    if (!restored) {
      flowie_test_record_store_destroy(&fixture);
      return;
    }
    check_int_eq(turbo_flow_start(restored), TURBO_OK);
    resumed = flowie_test_connect(second_port);
    check_true(resumed != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(resumed, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(resumed, 1u, 8u, 4096u), TURBO_OK);

    flowie_test_socket_close(resumed);
    check_int_eq(turbo_flow_stop(restored), TURBO_OK);
    turbo_flow_destroy(restored);
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-002 keeps the committed owner when CONNACK queue admission fails") {
    enum { CONNACK_REJECTING_SEND_HWM = 4u };
    unsigned short first_port = flowie_test_port();
    unsigned short second_port = flowie_test_port();
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    turbo_flow_t *first;
    turbo_flow_t *restored;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_t resumed = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t rejected[5];
    flowie_test_record_store_init(&fixture, &store);
    first = flowie_test_persistent_flow_with_send_hwm(first_port, &store,
                                                      CONNACK_REJECTING_SEND_HWM);
    check_int_gt(first_port, 0);
    check_int_gt(second_port, 0);
    check_not_null(first);
    check_int_eq(turbo_flow_start(first), TURBO_OK);
    client = flowie_test_connect(first_port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_ne(flowie_test_recv_exact(client, rejected, sizeof(rejected)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture, 1u), TURBO_OK);
    check_true(atomic_load_explicit(&fixture.present, memory_order_acquire));
    check_true(fixture.revision > TURBO_FLOW_RECORD_REVISION_ABSENT);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(first), TURBO_OK);
    turbo_flow_destroy(first);

    restored = flowie_test_persistent_flow(second_port, &store);
    check_not_null(restored);
    check_int_eq(turbo_flow_start(restored), TURBO_OK);
    resumed = flowie_test_connect(second_port);
    check_true(resumed != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(resumed, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(resumed, 1u, 8u, 4096u), TURBO_OK);
    flowie_test_socket_close(resumed);
    check_int_eq(turbo_flow_stop(restored), TURBO_OK);
    turbo_flow_destroy(restored);
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-003 permits one CAS generation and fences the stale writer") {
    static const uint8_t key[] = {'s', 0u, 'e', 's'};
    static const uint8_t first[] = {1u, 2u, 3u};
    static const uint8_t winner[] = {4u, 5u, 6u};
    static const uint8_t stale[] = {7u, 8u, 9u};
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    flowie_test_record_store_init(&fixture, &store);
    check_int_eq(flowie_test_record_put(&fixture, key, sizeof(key),
                                        TURBO_FLOW_RECORD_REVISION_ABSENT, 1u, first,
                                        sizeof(first)),
                 TURBO_OK);
    check_int_eq(flowie_test_record_put(&fixture, key, sizeof(key), 1u, 2u, winner,
                                        sizeof(winner)),
                 TURBO_OK);
    check_int_eq(flowie_test_record_put(&fixture, key, sizeof(key), 1u, 2u, stale, sizeof(stale)),
                 TURBO_EBUSY);
    check_uint_eq(fixture.revision, 2u);
    check_size_eq(fixture.key_size, sizeof(key));
    check_mem_eq(fixture.key, key, sizeof(key));
    check_size_eq(fixture.value_size, sizeof(winner));
    check_mem_eq(fixture.value, winner, sizeof(winner));
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-007 resolves a lost commit reply by scanning the committed revision") {
    unsigned short first_port = flowie_test_port();
    unsigned short second_port = flowie_test_port();
    check_int_gt(first_port, 0);
    check_int_gt(second_port, 0);
    check_int_eq(flowie_test_lost_commit_recovery(first_port, second_port), TURBO_OK);
  }

  it("MQTT-STORE-005 rebuilds retained put replace and delete at both commit sides") {
    static const uint8_t retained_x[] = {0x31u, 0x0bu, 0x00u, 0x07u, 's',   't', 'a',
                                         't',   'e',   '/',   'a',   0x00u, 'x'};
    static const uint8_t retained_z[] = {0x31u, 0x0bu, 0x00u, 0x07u, 's',   't', 'a',
                                         't',   'e',   '/',   'a',   0x00u, 'z'};
    static const uint8_t retained_delete[] = {0x31u, 0x0au, 0x00u, 0x07u, 's', 't',
                                              'a',   't',   'e',   '/',   'a', 0x00u};
    static const uint8_t subscribe_packets[][15] = {
        {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 's', 't', 'a', 't',
         'e',   '/',   'a',   0x00u},
        {0x82u, 0x0du, 0x00u, 0x02u, 0x00u, 0x00u, 0x07u, 's', 't', 'a', 't',
         'e',   '/',   'a',   0x00u},
        {0x82u, 0x0du, 0x00u, 0x03u, 0x00u, 0x00u, 0x07u, 's', 't', 'a', 't',
         'e',   '/',   'a',   0x00u},
        {0x82u, 0x0du, 0x00u, 0x04u, 0x00u, 0x00u, 0x07u, 's', 't', 'a', 't',
         'e',   '/',   'a',   0x00u}};
    static const uint8_t subacks[][6] = {{0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u},
                                         {0x90u, 0x04u, 0x00u, 0x02u, 0x00u, 0x00u},
                                         {0x90u, 0x04u, 0x00u, 0x03u, 0x00u, 0x00u},
                                         {0x90u, 0x04u, 0x00u, 0x04u, 0x00u, 0x00u}};
    flowie_test_record_store_pair_t fixture;
    turbo_flow_record_store_t store;
    turbo_flow_t *flow = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t received[32];
    unsigned short port;

    flowie_test_record_store_pair_init(&fixture, &store);
    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 0u, 8u, 4096u), TURBO_OK);

    atomic_store_explicit(&fixture.fail_retained_commits, 1, memory_order_release);
    check_int_eq(flowie_test_send(client, retained_x, sizeof(retained_x)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture.retained, 1u), TURBO_OK);
    check_false(atomic_load_explicit(&fixture.retained.present, memory_order_acquire));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    atomic_store_explicit(&fixture.fail_retained_commits, 0, memory_order_release);
    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(client, retained_x, sizeof(retained_x)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture.retained, 2u), TURBO_OK);
    check_true(atomic_load_explicit(&fixture.retained.present, memory_order_acquire));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(client, subscribe_packets[0], sizeof(subscribe_packets[0])),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(subacks[0])), TURBO_OK);
    check_mem_eq(received, subacks[0], sizeof(subacks[0]));
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(retained_x)), TURBO_OK);
    check_mem_eq(received, retained_x, sizeof(retained_x));

    atomic_store_explicit(&fixture.fail_retained_commits, 1, memory_order_release);
    check_int_eq(flowie_test_send(client, retained_delete, sizeof(retained_delete)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture.retained, 3u), TURBO_OK);
    check_true(atomic_load_explicit(&fixture.retained.present, memory_order_acquire));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    atomic_store_explicit(&fixture.fail_retained_commits, 0, memory_order_release);
    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(client, subscribe_packets[1], sizeof(subscribe_packets[1])),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(subacks[1])), TURBO_OK);
    check_mem_eq(received, subacks[1], sizeof(subacks[1]));
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(retained_x)), TURBO_OK);
    check_mem_eq(received, retained_x, sizeof(retained_x));

    atomic_store_explicit(&fixture.lose_retained_reply, 1, memory_order_release);
    check_int_eq(flowie_test_send(client, retained_z, sizeof(retained_z)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture.retained, 4u), TURBO_OK);
    check_uint_eq(fixture.retained.revision, 2u);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(client, subscribe_packets[2], sizeof(subscribe_packets[2])),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(subacks[2])), TURBO_OK);
    check_mem_eq(received, subacks[2], sizeof(subacks[2]));
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(retained_z)), TURBO_OK);
    check_mem_eq(received, retained_z, sizeof(retained_z));
    check_int_eq(flowie_test_send(client, retained_delete, sizeof(retained_delete)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture.retained, 5u), TURBO_OK);
    check_false(atomic_load_explicit(&fixture.retained.present, memory_order_acquire));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    check_int_eq(flowie_test_send(client, subscribe_packets[3], sizeof(subscribe_packets[3])),
                 TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received, sizeof(subacks[3])), TURBO_OK);
    check_mem_eq(received, subacks[3], sizeof(subacks[3]));
    check_false(flowie_test_socket_readable(client, 100u));
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_test_record_store_pair_destroy(&fixture);
  }

  it("MQTT-STORE-005 rebuilds pending Will schedule and cancel at both commit sides") {
    static const char client_id[] = "will-durable";
    static const uint8_t session_expiry[] = {
        FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL, 0x00u, 0x00u, 0x00u, 0x3cu};
    static const uint8_t will_delay[] = {
        FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL, 0x00u, 0x00u, 0x00u, 0x1eu};
    static const uint8_t normal_disconnect[] = {0xe0u, 0x00u};
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    turbo_flow_t *flow = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t connect_wire[192];
    size_t connect_size = 0u;
    size_t expected_commits;
    unsigned short port;
    int has_will = 0;
    int will_pending = 0;

    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 0u;
    connect.keep_alive = 60u;
    connect.client_id =
        (flowie_mqtt_span_t){(const uint8_t *)client_id, sizeof(client_id) - 1u};
    connect.properties = (flowie_mqtt_span_t){session_expiry, sizeof(session_expiry)};
    connect.has_will = 1u;
    connect.will_qos = 1u;
    connect.will_properties = (flowie_mqtt_span_t){will_delay, sizeof(will_delay)};
    connect.will_topic = (flowie_mqtt_span_t){(const uint8_t *)"will/state", 10u};
    connect.will_payload = (flowie_mqtt_span_t){(const uint8_t *)"offline", 7u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_wire, sizeof(connect_wire),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    flowie_test_record_store_init(&fixture, &store);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_wire, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 0u, 8u, 4096u), TURBO_OK);
    expected_commits = atomic_load_explicit(&fixture.commit_calls, memory_order_acquire) + 1u;
    atomic_store_explicit(&fixture.fail_commits, 1, memory_order_release);
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_test_wait_commits(&fixture, expected_commits), TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    atomic_store_explicit(&fixture.fail_commits, 0, memory_order_release);
    check_int_eq(flowie_test_restore_will_state(&fixture, client_id, &has_will, &will_pending),
                 TURBO_OK);
    check_true(has_will);
    /* A persisted active connection becomes an abnormal disconnect after broker restart, so
     * its Will must remain recoverable even when the explicit close commit failed. */
    check_true(will_pending);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, connect_wire, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    expected_commits = atomic_load_explicit(&fixture.commit_calls, memory_order_acquire) + 1u;
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_test_wait_commits(&fixture, expected_commits), TURBO_OK);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    check_int_eq(flowie_test_restore_will_state(&fixture, client_id, &has_will, &will_pending),
                 TURBO_OK);
    check_true(has_will);
    check_true(will_pending);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    atomic_store_explicit(&fixture.fail_commits, 1, memory_order_release);
    expected_commits = atomic_load_explicit(&fixture.commit_calls, memory_order_acquire) + 1u;
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, connect_wire, connect_size), TURBO_OK);
    {
      uint8_t rejected[5];
      check_int_ne(flowie_test_recv_exact(client, rejected, sizeof(rejected)), TURBO_OK);
    }
    check_int_eq(flowie_test_wait_commits(&fixture, expected_commits), TURBO_OK);
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flow = NULL;
    atomic_store_explicit(&fixture.fail_commits, 0, memory_order_release);
    check_int_eq(flowie_test_restore_will_state(&fixture, client_id, &has_will, &will_pending),
                 TURBO_OK);
    check_true(has_will);
    check_true(will_pending);

    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    client = flowie_test_connect(port);
    check_int_eq(flowie_test_send(client, connect_wire, connect_size), TURBO_OK);
    check_int_eq(flowie_test_recv_mqtt5_connack(client, 1u, 8u, 4096u), TURBO_OK);
    expected_commits = atomic_load_explicit(&fixture.commit_calls, memory_order_acquire) + 1u;
    check_int_eq(flowie_test_send(client, normal_disconnect, sizeof(normal_disconnect)), TURBO_OK);
    check_int_eq(flowie_test_wait_commits(&fixture, expected_commits), TURBO_OK);
    flowie_test_socket_close(client);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_int_eq(flowie_test_restore_will_state(&fixture, client_id, &has_will, &will_pending),
                 TURBO_OK);
    check_false(has_will);
    check_false(will_pending);
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-006 fails before listen when scan is unavailable or a record is corrupt") {
    unsigned short port = flowie_test_port();
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    turbo_flow_t *flow;
    flowie_test_record_store_init(&fixture, &store);
    atomic_store_explicit(&fixture.scan_status, TURBO_EIO, memory_order_release);
    flow = flowie_test_persistent_flow(port, &store);
    check_null(flow);
    atomic_store_explicit(&fixture.scan_status, TURBO_OK, memory_order_release);
    fixture.key = (uint8_t *)malloc(3u);
    fixture.value = (uint8_t *)malloc(8u);
    check_not_null(fixture.key);
    check_not_null(fixture.value);
    if (fixture.key && fixture.value) {
      memcpy(fixture.key, "bad", 3u);
      memcpy(fixture.value, "FSES\0\2\0\0", 8u);
      fixture.key_size = 3u;
      fixture.value_size = 8u;
      fixture.revision = 1u;
      atomic_store_explicit(&fixture.present, 1, memory_order_release);
      flow = flowie_test_persistent_flow(flowie_test_port(), &store);
      check_null(flow);
      check_mem_eq(fixture.value, "FSES\0\2\0\0", 8u);
      fixture.value_size = 7u;
      flow = flowie_test_persistent_flow(flowie_test_port(), &store);
      check_null(flow);
      check_size_eq(fixture.value_size, 7u);
    }
    flowie_test_record_store_destroy(&fixture);

    /* Build one valid record, then prove duplicate fields and provider-overlimit values fail. */
    flowie_test_record_store_init(&fixture, &store);
    port = flowie_test_port();
    flow = flowie_test_persistent_flow(port, &store);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    {
      flowie_test_socket_t client = flowie_test_connect(port);
      check_true(client != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                   TURBO_OK);
      check_int_eq(flowie_test_recv_mqtt5_connack(client, 0u, 8u, 4096u), TURBO_OK);
      flowie_test_socket_close(client);
    }
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    check_true(atomic_load_explicit(&fixture.present, memory_order_acquire));
    {
      turbo_ltv_message_t *first = NULL;
      uint32_t first_payload_size = 0u;
      size_t first_header_size = 0u;
      size_t first_wire_size;
      size_t valid_size = fixture.value_size;
      uint8_t *conflicting;
      check_int_eq(turbo_ltv_peek_size(fixture.value, fixture.value_size, &first_payload_size,
                                       &first_header_size),
                   TURBO_OK);
      check_int_eq(turbo_parse_ltv(fixture.value, first_header_size + first_payload_size, &first),
                   TURBO_OK);
      check_not_null(first);
      first_wire_size = first_header_size + first_payload_size;
      check_true(first_wire_size != 0u && first_wire_size <= valid_size);
      turbo_free_ltv(&first);
      conflicting = (uint8_t *)realloc(fixture.value, valid_size + first_wire_size);
      check_not_null(conflicting);
      if (conflicting) {
        fixture.value = conflicting;
        memcpy(fixture.value + valid_size, fixture.value, first_wire_size);
        fixture.value_size = valid_size + first_wire_size;
        flow = flowie_test_persistent_flow(flowie_test_port(), &store);
        check_null(flow);
        fixture.value_size = valid_size;
      }
      conflicting = (uint8_t *)realloc(fixture.value, FLOWIE_STORE_TEST_MAX_VALUE_SIZE + 1u);
      check_not_null(conflicting);
      if (conflicting) {
        fixture.value = conflicting;
        memset(fixture.value + valid_size, 0,
               FLOWIE_STORE_TEST_MAX_VALUE_SIZE + 1u - valid_size);
        fixture.value_size = FLOWIE_STORE_TEST_MAX_VALUE_SIZE + 1u;
        flow = flowie_test_persistent_flow(flowie_test_port(), &store);
        check_null(flow);
      }
    }
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-008 prevents stale expiry cleanup from deleting a newer revision") {
    static const uint8_t key[] = "expiry/session";
    static const uint8_t expired[] = "old";
    static const uint8_t refreshed[] = "new";
    flowie_test_record_store_t fixture;
    flowie_test_expiry_cleanup_t cleanup;
    turbo_flow_record_store_t store;
    turbo_thread_t cleanup_thread;
    flowie_test_record_store_init(&fixture, &store);
    check_int_eq(flowie_test_record_put(&fixture, key, sizeof(key) - 1u, 0u, 1u, expired,
                                        sizeof(expired) - 1u),
                 TURBO_OK);
    memset(&cleanup, 0, sizeof(cleanup));
    cleanup.store = &fixture;
    cleanup.key = key;
    cleanup.key_size = sizeof(key) - 1u;
    cleanup.expected_revision = 1u;
    atomic_init(&cleanup.ready, 0);
    atomic_init(&cleanup.proceed, 0);
    atomic_init(&cleanup.status, TURBO_EBUSY);
    check_int_eq(turbo_thread_create(&cleanup_thread, flowie_test_expiry_cleanup_thread, &cleanup),
                 TURBO_OK);
    while (!atomic_load_explicit(&cleanup.ready, memory_order_acquire)) turbo_thread_yield();
    check_int_eq(flowie_test_record_put(&fixture, key, sizeof(key) - 1u, 1u, 2u, refreshed,
                                        sizeof(refreshed) - 1u),
                 TURBO_OK);
    atomic_store_explicit(&cleanup.proceed, 1, memory_order_release);
    check_int_eq(turbo_thread_join(&cleanup_thread), TURBO_OK);
    turbo_thread_destroy(&cleanup_thread);
    check_int_eq(atomic_load_explicit(&cleanup.status, memory_order_acquire), TURBO_EBUSY);
    check_true(atomic_load_explicit(&fixture.present, memory_order_acquire));
    check_uint_eq(fixture.revision, 2u);
    check_mem_eq(fixture.value, refreshed, sizeof(refreshed) - 1u);
    flowie_test_record_store_destroy(&fixture);
  }

  it("MQTT-STORE-009 preserves binary keys and maximum bounded values byte for byte") {
    static const uint8_t key_prefix[] = {0u, 0xffu, 'r', 0u};
    flowie_test_record_store_t fixture;
    turbo_flow_record_store_t store;
    uint8_t *key;
    uint8_t *value;
    flowie_test_record_store_init(&fixture, &store);
    key = (uint8_t *)malloc(FLOWIE_STORE_TEST_MAX_KEY_SIZE);
    value = (uint8_t *)malloc(FLOWIE_STORE_TEST_MAX_VALUE_SIZE);
    check_not_null(key);
    check_not_null(value);
    if (key && value) {
      for (size_t i = 0u; i < FLOWIE_STORE_TEST_MAX_KEY_SIZE; ++i) key[i] = (uint8_t)i;
      for (size_t i = 0u; i < FLOWIE_STORE_TEST_MAX_VALUE_SIZE; ++i)
        value[i] = (uint8_t)(i * 17u + 3u);
      memcpy(key, key_prefix, sizeof(key_prefix));
      check_int_eq(flowie_test_record_put(&fixture, key, FLOWIE_STORE_TEST_MAX_KEY_SIZE, 0u, 1u,
                                          value, FLOWIE_STORE_TEST_MAX_VALUE_SIZE),
                   TURBO_OK);
      check_size_eq(fixture.key_size, FLOWIE_STORE_TEST_MAX_KEY_SIZE);
      check_size_eq(fixture.value_size, FLOWIE_STORE_TEST_MAX_VALUE_SIZE);
      check_mem_eq(fixture.key, key, FLOWIE_STORE_TEST_MAX_KEY_SIZE);
      check_mem_eq(fixture.value, value, FLOWIE_STORE_TEST_MAX_VALUE_SIZE);
    }
    free(key);
    free(value);
    flowie_test_record_store_destroy(&fixture);
  }
}
