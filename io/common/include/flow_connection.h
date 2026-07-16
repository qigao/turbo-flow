#ifndef FLOW_CONNECTION_H
#define FLOW_CONNECTION_H

#include "turbo_flow.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tf_connection_state_s {
  char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
  atomic_int state;
  atomic_int last_status;
  atomic_uint_fast64_t connections_current;
  atomic_uint_fast64_t connection_limit;
  atomic_uint_fast64_t in_flight_messages;
  atomic_uint_fast64_t in_flight_bytes;
} tf_connection_state_t;

int tf_connection_init(tf_connection_state_t *connection, const char *endpoint,
                       uint64_t connection_limit);
int tf_connection_set_endpoint(tf_connection_state_t *connection, const char *endpoint);
void tf_connection_transition(tf_connection_state_t *connection,
                              turbo_flow_connection_state_t state, int status);
void tf_connection_set_usage(tf_connection_state_t *connection, uint64_t connections_current,
                             uint64_t in_flight_messages, uint64_t in_flight_bytes);
int tf_connection_request_begin(tf_connection_state_t *connection, uint64_t bytes);
int tf_connection_request_end(tf_connection_state_t *connection, uint64_t bytes);
int tf_connection_snapshot(const tf_connection_state_t *connection,
                           turbo_flow_connection_snapshot_t *out);

#ifdef __cplusplus
}
#endif

#endif
