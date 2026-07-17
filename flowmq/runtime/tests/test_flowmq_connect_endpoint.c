#include "flowmq_connect_endpoint.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdatomic.h>
#include <string.h>

typedef struct client_event_capture_s {
  atomic_int failures;
  atomic_int stopped;
  atomic_int last_status;
} client_event_capture_t;

static void capture_state(void *ctx, flowmq_connect_endpoint_connection_state_t state, int status,
                          size_t connections_current) {
  client_event_capture_t *capture = (client_event_capture_t *)ctx;
  (void)connections_current;
  if (state == FLOWMQ_ENDPOINT_CONNECTION_STOPPED)
    atomic_fetch_add_explicit(&capture->stopped, 1, memory_order_acq_rel);
  atomic_store_explicit(&capture->last_status, status, memory_order_release);
}

static void capture_event(void *ctx, const flowmq_connect_endpoint_event_t *event) {
  client_event_capture_t *capture = (client_event_capture_t *)ctx;
  if (event->kind == FLOWMQ_ENDPOINT_EVENT_RECONNECT_FAILED)
    atomic_fetch_add_explicit(&capture->failures, 1, memory_order_acq_rel);
}

static flowmq_connect_endpoint_config_t private_config(client_event_capture_t *capture) {
  flowmq_connect_endpoint_config_t config;
  memset(&config, 0, sizeof(config));
  config.transport = FLOWMQ_TRANSPORT_TCP;
  config.pattern = FLOWMQ_PROTOCOL_SUB;
  config.host = "127.0.0.1";
  config.path = "";
  config.topic = "";
  config.identity = "client-test";
  config.port = 1;
  config.max_frame_size = 4096u;
  config.timeouts.timeout_ms = 20u;
  config.timeouts.connect_timeout_ms = 20u;
  config.reconnect_initial_ms = 0u;
  config.on_state = capture_state;
  config.on_event = capture_event;
  config.callback_ctx = capture;
  return config;
}

spec("flowmq_connect_endpoint owner") {
  static coro_context_t *context;

  before_all() { context = coro_context_create(NULL); }

  after_all() {
    coro_context_destroy(context);
    context = NULL;
  }

  it("rejects incomplete ownership and endpoint configuration") {
    flowmq_connect_endpoint_config_t config;
    flowmq_connect_endpoint_t *client = NULL;
    memset(&config, 0, sizeof(config));
    check_int_eq(flowmq_connect_endpoint_create(&config, &client), TURBO_EINVAL);
    check_null(client);
  }

  it("creates and destroys an idle private context") {
    client_event_capture_t capture;
    flowmq_connect_endpoint_config_t config;
    flowmq_connect_endpoint_t *client = NULL;
    memset(&capture, 0, sizeof(capture));
    config = private_config(&capture);
    config.context = NULL;
    config.drive_context = 1;
    config.own_context = 1;
    check_int_eq(flowmq_connect_endpoint_create(&config, &client), TURBO_OK);
    check_not_null(client);
    flowmq_connect_endpoint_destroy(client);
  }

  it("updates an endpoint only while stopped") {
    client_event_capture_t capture;
    flowmq_connect_endpoint_config_t config;
    flowmq_connect_endpoint_t *client = NULL;
    memset(&capture, 0, sizeof(capture));
    config = private_config(&capture);
    config.context = context;
    check_int_eq(flowmq_connect_endpoint_create(&config, &client), TURBO_OK);
    check_int_eq(flowmq_connect_endpoint_update_endpoint(client, "localhost", 2, "/fmq"), TURBO_OK);
    check_int_eq(flowmq_connect_endpoint_update_endpoint(client, NULL, 2, "/fmq"), TURBO_EINVAL);
    flowmq_connect_endpoint_destroy(client);
  }

  it("keeps REQ exchange state inside the owner") {
    client_event_capture_t capture;
    flowmq_connect_endpoint_config_t config;
    flowmq_connect_endpoint_t *client = NULL;
    flowmq_peer_exchange_state_t state;
    uint64_t generation = 0u;
    uint64_t correlation_id = 0u;
    memset(&capture, 0, sizeof(capture));
    config = private_config(&capture);
    config.context = context;
    config.pattern = FLOWMQ_PROTOCOL_REQ;
    check_int_eq(flowmq_connect_endpoint_create(&config, &client), TURBO_OK);
    check_int_eq(flowmq_connect_endpoint_request_begin(client, 41u, &generation), TURBO_OK);
    check_int_eq(
        flowmq_connect_endpoint_exchange_snapshot(client, &state, &generation, &correlation_id),
        TURBO_OK);
    check_int_eq(state, FLOWMQ_PEER_EXCHANGE_WAIT_REPLY);
    check_uint_eq(correlation_id, 41u);
    check_int_eq(flowmq_connect_endpoint_request_finish(client, generation, correlation_id,
                                                        FLOWMQ_PEER_EXCHANGE_READY),
                 TURBO_OK);
    flowmq_connect_endpoint_destroy(client);
  }
}
