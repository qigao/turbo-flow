#include "flow_connection.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flow_connection") {
  it("publishes one typed connection snapshot") {
    tf_connection_state_t connection;
    turbo_flow_connection_snapshot_t snapshot;
    memset(&snapshot, 0, sizeof(snapshot));

    check_int_eq(tf_connection_init(&connection, "redis://127.0.0.1:6379", 8), TURBO_OK);
    tf_connection_transition(&connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
    tf_connection_set_usage(&connection, 1, 3, 1024);

    check_int_eq(tf_connection_snapshot(&connection, &snapshot), TURBO_OK);
    check_str_eq(snapshot.endpoint, "redis://127.0.0.1:6379");
    check_int_eq(snapshot.state, TURBO_FLOW_CONNECTION_READY);
    check_int_eq(snapshot.last_status, TURBO_OK);
    check_uint_eq(snapshot.connections_current, 1);
    check_uint_eq(snapshot.connection_limit, 8);
    check_uint_eq(snapshot.in_flight_messages, 3);
    check_uint_eq(snapshot.in_flight_bytes, 1024);
  }

  it("rejects endpoints that cannot fit the public snapshot ABI") {
    tf_connection_state_t connection;
    char endpoint[TURBO_FLOW_ENDPOINT_MAX + 2u];
    memset(endpoint, 'x', sizeof(endpoint));
    endpoint[sizeof(endpoint) - 1u] = '\0';

    check_int_eq(tf_connection_init(&connection, endpoint, 0), TURBO_ENOSPC);
    check_int_eq(tf_connection_init(&connection, "tcp://localhost:1", 0), TURBO_OK);
    check_int_eq(tf_connection_set_endpoint(&connection, endpoint), TURBO_ENOSPC);
  }

  it("tracks concurrent request load without underflow") {
    tf_connection_state_t connection;
    turbo_flow_connection_snapshot_t snapshot;
    check_int_eq(tf_connection_init(&connection, "http://localhost:8080", 1), TURBO_OK);
    check_int_eq(tf_connection_request_begin(&connection, 10), TURBO_OK);
    check_int_eq(tf_connection_request_begin(&connection, 20), TURBO_OK);
    check_int_eq(tf_connection_snapshot(&connection, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.in_flight_messages, 2);
    check_uint_eq(snapshot.in_flight_bytes, 30);
    check_int_eq(tf_connection_request_end(&connection, 10), TURBO_OK);
    check_int_eq(tf_connection_request_end(&connection, 20), TURBO_OK);
    check_int_eq(tf_connection_request_end(&connection, 1), TURBO_ERANGE);
    check_int_eq(tf_connection_snapshot(&connection, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.in_flight_messages, 0);
    check_uint_eq(snapshot.in_flight_bytes, 0);
  }
}
