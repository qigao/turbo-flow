#include "flow_connection.h"

#include "turbo_error.h"

#include <stdint.h>
#include <string.h>

static int tf_connection_state_valid(turbo_flow_connection_state_t state) {
  return state >= TURBO_FLOW_CONNECTION_STOPPED && state <= TURBO_FLOW_CONNECTION_FAILED;
}

int tf_connection_set_endpoint(tf_connection_state_t *connection, const char *endpoint) {
  size_t length;
  if (!connection || !endpoint) return TURBO_EINVAL;
  length = strlen(endpoint);
  if (length > TURBO_FLOW_ENDPOINT_MAX) return TURBO_ENOSPC;
  memcpy(connection->endpoint, endpoint, length + 1u);
  return TURBO_OK;
}

int tf_connection_init(tf_connection_state_t *connection, const char *endpoint,
                       uint64_t connection_limit) {
  int rc;
  if (!connection) return TURBO_EINVAL;
  memset(connection, 0, sizeof(*connection));
  rc = tf_connection_set_endpoint(connection, endpoint ? endpoint : "");
  if (rc != TURBO_OK) return rc;
  atomic_init(&connection->state, TURBO_FLOW_CONNECTION_STOPPED);
  atomic_init(&connection->last_status, TURBO_ENOTCONN);
  atomic_init(&connection->connections_current, 0u);
  atomic_init(&connection->connection_limit, connection_limit);
  atomic_init(&connection->in_flight_messages, 0u);
  atomic_init(&connection->in_flight_bytes, 0u);
  return TURBO_OK;
}

void tf_connection_transition(tf_connection_state_t *connection,
                              turbo_flow_connection_state_t state, int status) {
  if (!connection || !tf_connection_state_valid(state)) return;
  atomic_store_explicit(&connection->last_status, status, memory_order_relaxed);
  atomic_store_explicit(&connection->state, state, memory_order_release);
}

void tf_connection_set_usage(tf_connection_state_t *connection, uint64_t connections_current,
                             uint64_t in_flight_messages, uint64_t in_flight_bytes) {
  if (!connection) return;
  atomic_store_explicit(&connection->connections_current, connections_current,
                        memory_order_relaxed);
  atomic_store_explicit(&connection->in_flight_messages, in_flight_messages,
                        memory_order_relaxed);
  atomic_store_explicit(&connection->in_flight_bytes, in_flight_bytes, memory_order_relaxed);
}

static int tf_connection_counter_add(atomic_uint_fast64_t *counter, uint64_t value) {
  uint_fast64_t current;
  if (!counter) return TURBO_EINVAL;
  current = atomic_load_explicit(counter, memory_order_relaxed);
  for (;;) {
    if (value > UINT64_MAX - current) return TURBO_ERANGE;
    if (atomic_compare_exchange_weak_explicit(counter, &current, current + value,
                                              memory_order_relaxed, memory_order_relaxed)) {
      return TURBO_OK;
    }
  }
}

static int tf_connection_counter_sub(atomic_uint_fast64_t *counter, uint64_t value) {
  uint_fast64_t current;
  if (!counter) return TURBO_EINVAL;
  current = atomic_load_explicit(counter, memory_order_relaxed);
  for (;;) {
    if (current < value) return TURBO_ERANGE;
    if (atomic_compare_exchange_weak_explicit(counter, &current, current - value,
                                              memory_order_relaxed, memory_order_relaxed)) {
      return TURBO_OK;
    }
  }
}

int tf_connection_request_begin(tf_connection_state_t *connection, uint64_t bytes) {
  int rc;
  if (!connection) return TURBO_EINVAL;
  rc = tf_connection_counter_add(&connection->in_flight_messages, 1u);
  if (rc != TURBO_OK) return rc;
  rc = tf_connection_counter_add(&connection->in_flight_bytes, bytes);
  if (rc != TURBO_OK) {
    (void)tf_connection_counter_sub(&connection->in_flight_messages, 1u);
  }
  return rc;
}

int tf_connection_request_end(tf_connection_state_t *connection, uint64_t bytes) {
  int rc;
  if (!connection) return TURBO_EINVAL;
  rc = tf_connection_counter_sub(&connection->in_flight_bytes, bytes);
  if (rc != TURBO_OK) return rc;
  rc = tf_connection_counter_sub(&connection->in_flight_messages, 1u);
  if (rc != TURBO_OK) {
    (void)tf_connection_counter_add(&connection->in_flight_bytes, bytes);
  }
  return rc;
}

int tf_connection_snapshot(const tf_connection_state_t *connection,
                           turbo_flow_connection_snapshot_t *out) {
  if (!connection || !out) return TURBO_EINVAL;
  memcpy(out->endpoint, connection->endpoint, sizeof(out->endpoint));
  out->state = (turbo_flow_connection_state_t)atomic_load_explicit(
      &connection->state, memory_order_acquire);
  out->last_status = atomic_load_explicit(&connection->last_status, memory_order_relaxed);
  out->connections_current =
      atomic_load_explicit(&connection->connections_current, memory_order_relaxed);
  out->connection_limit =
      atomic_load_explicit(&connection->connection_limit, memory_order_relaxed);
  out->in_flight_messages =
      atomic_load_explicit(&connection->in_flight_messages, memory_order_relaxed);
  out->in_flight_bytes =
      atomic_load_explicit(&connection->in_flight_bytes, memory_order_relaxed);
  return TURBO_OK;
}
