#include "flowie_cluster_broadcast_target_dispatch_internal.h"

#include "tinytest.h"
#include "turbo_thread.h"

#include <string.h>

enum {
  TARGET_TEST_PAYLOAD_SIZE = FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE + 1u,
  TARGET_TEST_WAIT_STEPS = 1000u,
};

typedef struct flowie_cluster_target_test_state_s {
  uint8_t payload[TARGET_TEST_PAYLOAD_SIZE + 1u];
  size_t payload_size;
  uint64_t token;
  int available;
  int apply_status[4];
  size_t apply_status_count;
  int ack_status[4];
  size_t ack_status_count;
  int requeue_status[4];
  size_t requeue_status_count;
  size_t claims;
  size_t applies;
  size_t acks;
  size_t requeues;
  uint64_t ack_token[4];
  uint64_t requeue_token[4];
  uint8_t copied_payload[TARGET_TEST_PAYLOAD_SIZE + 1u];
  size_t copied_payload_size;
} flowie_cluster_target_test_state_t;

static int flowie_cluster_target_test_claim(
    void *ctx, flowie_cluster_broadcast_target_claim_t *out) {
  flowie_cluster_target_test_state_t *state = (flowie_cluster_target_test_state_t *)ctx;
  if (!state || !out || out->size != sizeof(*out) || !state->available) return TURBO_ENOENT;
  state->available = 0;
  state->claims += 1u;
  out->token = state->token;
  out->payload = tstr_v_from_buf((const char *)state->payload, state->payload_size);
  return TURBO_OK;
}

static int flowie_cluster_target_test_ack(void *ctx, uint64_t token) {
  flowie_cluster_target_test_state_t *state = (flowie_cluster_target_test_state_t *)ctx;
  size_t slot;
  if (!state || state->acks >= sizeof(state->ack_token) / sizeof(state->ack_token[0]))
    return TURBO_EPROTO;
  slot = state->acks++;
  state->ack_token[slot] = token;
  return slot < state->ack_status_count ? state->ack_status[slot] : TURBO_OK;
}

static int flowie_cluster_target_test_requeue(void *ctx, uint64_t token) {
  flowie_cluster_target_test_state_t *state = (flowie_cluster_target_test_state_t *)ctx;
  size_t slot;
  if (!state || state->requeues >= sizeof(state->requeue_token) / sizeof(state->requeue_token[0]))
    return TURBO_EPROTO;
  slot = state->requeues++;
  state->requeue_token[slot] = token;
  if (slot < state->requeue_status_count && state->requeue_status[slot] != TURBO_OK)
    return state->requeue_status[slot];
  state->available = 1;
  return TURBO_OK;
}

static int flowie_cluster_target_test_apply(
    void *ctx, const void *payload, size_t payload_size,
    flowie_cluster_broadcast_target_apply_complete_fn complete, void *completion_ctx) {
  flowie_cluster_target_test_state_t *state = (flowie_cluster_target_test_state_t *)ctx;
  size_t slot;
  int status;
  if (!state || !payload || payload_size > sizeof(state->copied_payload) || !complete)
    return TURBO_EINVAL;
  slot = state->applies++;
  memcpy(state->copied_payload, payload, payload_size);
  state->copied_payload_size = payload_size;
  status = slot < state->apply_status_count ? state->apply_status[slot] : TURBO_OK;
  complete(completion_ctx, status);
  return TURBO_OK;
}

static flowie_cluster_broadcast_target_dispatcher_config_t
flowie_cluster_target_test_config(flowie_cluster_target_test_state_t *state) {
  flowie_cluster_broadcast_target_dispatcher_config_t config =
      FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CONFIG_INIT;
  config.max_payload_size = sizeof(state->payload);
  config.poll_interval_ns = UINT64_C(1000000);
  config.retry_interval_ns = UINT64_C(1000000);
  config.ack_retry_interval_ns = UINT64_C(1000000);
  config.claim = flowie_cluster_target_test_claim;
  config.ack = flowie_cluster_target_test_ack;
  config.requeue = flowie_cluster_target_test_requeue;
  config.transport_ctx = state;
  config.apply = flowie_cluster_target_test_apply;
  config.apply_ctx = state;
  return config;
}

static int flowie_cluster_target_test_wait(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, uint64_t acknowledged,
    flowie_cluster_broadcast_target_dispatcher_snapshot_t *out) {
  for (size_t step = 0u; step < TARGET_TEST_WAIT_STEPS; ++step) {
    int rc = flowie_cluster_broadcast_target_dispatcher_snapshot(dispatcher, out);
    if (rc != TURBO_OK || out->acknowledged_events >= acknowledged ||
        out->state == FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSED)
      return rc;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static void flowie_cluster_target_test_state_init(flowie_cluster_target_test_state_t *state) {
  memset(state, 0, sizeof(*state));
  memset(state->payload, 0x5a, sizeof(state->payload));
  state->payload_size = TARGET_TEST_PAYLOAD_SIZE;
  state->token = UINT64_C(73);
  state->available = 1;
}

spec("flowie cluster broadcast target transport dispatcher") {
  it("acknowledges a claim only after the owner apply completes durably") {
    flowie_cluster_target_test_state_t state;
    flowie_cluster_broadcast_target_dispatcher_config_t config;
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher = NULL;
    flowie_cluster_broadcast_target_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_SNAPSHOT_INIT;

    flowie_cluster_target_test_state_init(&state);
    config = flowie_cluster_target_test_config(&state);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_create(&config, &dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_target_test_wait(dispatcher, 1u, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.claimed_events, 1u);
    check_uint_eq(snapshot.apply_attempts, 1u);
    check_uint_eq(snapshot.ack_attempts, 1u);
    check_uint_eq(snapshot.acknowledged_events, 1u);
    check_uint_eq(snapshot.active_token, 0u);
    check_size_eq(state.claims, 1u);
    check_size_eq(state.applies, 1u);
    check_size_eq(state.acks, 1u);
    check_size_eq(state.requeues, 0u);
    check_uint_eq(state.ack_token[0], state.token);
    check_size_eq(state.copied_payload_size, state.payload_size);
    check_mem_eq(state.copied_payload, state.payload, state.payload_size);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_close(dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_drain(dispatcher, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_destroy(dispatcher), TURBO_OK);
  }

  it("requeues a retryable owner failure and reapplies the same immutable event") {
    flowie_cluster_target_test_state_t state;
    flowie_cluster_broadcast_target_dispatcher_config_t config;
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher = NULL;
    flowie_cluster_broadcast_target_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_SNAPSHOT_INIT;

    flowie_cluster_target_test_state_init(&state);
    state.apply_status[0] = TURBO_EIO;
    state.apply_status[1] = TURBO_OK;
    state.apply_status_count = 2u;
    config = flowie_cluster_target_test_config(&state);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_create(&config, &dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_target_test_wait(dispatcher, 1u, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.claimed_events, 2u);
    check_uint_eq(snapshot.apply_attempts, 2u);
    check_uint_eq(snapshot.requeued_events, 1u);
    check_uint_eq(snapshot.acknowledged_events, 1u);
    check_size_eq(state.requeues, 1u);
    check_uint_eq(state.requeue_token[0], state.token);
    check_size_eq(state.acks, 1u);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_close(dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_drain(dispatcher, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_destroy(dispatcher), TURBO_OK);
  }

  it("retries an uncertain acknowledgement against the exact active token") {
    flowie_cluster_target_test_state_t state;
    flowie_cluster_broadcast_target_dispatcher_config_t config;
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher = NULL;
    flowie_cluster_broadcast_target_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_SNAPSHOT_INIT;

    flowie_cluster_target_test_state_init(&state);
    state.ack_status[0] = TURBO_EIO;
    state.ack_status[1] = TURBO_OK;
    state.ack_status_count = 2u;
    config = flowie_cluster_target_test_config(&state);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_create(&config, &dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_target_test_wait(dispatcher, 1u, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.claimed_events, 1u);
    check_uint_eq(snapshot.apply_attempts, 1u);
    check_uint_eq(snapshot.ack_attempts, 2u);
    check_size_eq(state.acks, 2u);
    check_uint_eq(state.ack_token[0], state.token);
    check_uint_eq(state.ack_token[1], state.token);
    check_size_eq(state.requeues, 0u);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_close(dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_drain(dispatcher, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_destroy(dispatcher), TURBO_OK);
  }

  it("retries an uncertain requeue against the exact active token") {
    flowie_cluster_target_test_state_t state;
    flowie_cluster_broadcast_target_dispatcher_config_t config;
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher = NULL;
    flowie_cluster_broadcast_target_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_SNAPSHOT_INIT;

    flowie_cluster_target_test_state_init(&state);
    state.apply_status[0] = TURBO_EIO;
    state.apply_status[1] = TURBO_OK;
    state.apply_status_count = 2u;
    state.requeue_status[0] = TURBO_ETIMEDOUT;
    state.requeue_status[1] = TURBO_OK;
    state.requeue_status_count = 2u;
    config = flowie_cluster_target_test_config(&state);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_create(&config, &dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_target_test_wait(dispatcher, 1u, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.claimed_events, 2u);
    check_uint_eq(snapshot.apply_attempts, 2u);
    check_uint_eq(snapshot.requeued_events, 1u);
    check_size_eq(state.requeues, 2u);
    check_uint_eq(state.requeue_token[0], state.token);
    check_uint_eq(state.requeue_token[1], state.token);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_close(dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_drain(dispatcher, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_destroy(dispatcher), TURBO_OK);
  }

  it("fails closed and preserves the active token for an oversized claimed payload") {
    flowie_cluster_target_test_state_t state;
    flowie_cluster_broadcast_target_dispatcher_config_t config;
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher = NULL;
    flowie_cluster_broadcast_target_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_SNAPSHOT_INIT;

    flowie_cluster_target_test_state_init(&state);
    state.payload_size = sizeof(state.payload);
    config = flowie_cluster_target_test_config(&state);
    config.max_payload_size = TARGET_TEST_PAYLOAD_SIZE;
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_create(&config, &dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_target_test_wait(dispatcher, UINT64_MAX, &snapshot), TURBO_OK);
    check_int_eq(snapshot.state, FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSED);
    check_int_eq(snapshot.last_status, TURBO_EMSGSIZE);
    check_uint_eq(snapshot.claimed_events, 1u);
    check_uint_eq(snapshot.active_token, state.token);
    check_size_eq(state.applies, 0u);
    check_size_eq(state.acks, 0u);
    check_size_eq(state.requeues, 0u);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_close(dispatcher), TURBO_EALREADY);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_drain(dispatcher, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_dispatcher_destroy(dispatcher), TURBO_OK);
  }
}
