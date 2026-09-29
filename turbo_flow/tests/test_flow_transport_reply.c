#include "tinytest.h"
#include "turbo_flow.h"

#include <string.h>

typedef struct reply_fixture_s {
  turbo_flow_transport_reply_session_t captured;
  turbo_flow_transport_reply_terminal_t terminal;
  size_t sends;
  int terminal_ready;
} reply_fixture_t;

static int reply_capture(void *ctx, const turbo_flow_msg_t *message,
                         turbo_flow_transport_reply_session_t *session) {
  reply_fixture_t *fixture = (reply_fixture_t *)ctx;
  uint64_t generation = UINT64_C(0x1020304050607080);
  if (!fixture || !message || !session) return SALTS_EINVAL;
  *session = (turbo_flow_transport_reply_session_t)TURBO_FLOW_TRANSPORT_REPLY_SESSION_INIT;
  session->token_size = sizeof(generation);
  memcpy(session->token, &generation, sizeof(generation));
  fixture->captured = *session;
  return SALTS_OK;
}

static int reply_send(void *ctx, const turbo_flow_transport_reply_request_t *request) {
  reply_fixture_t *fixture = (reply_fixture_t *)ctx;
  if (!fixture || !request || fixture->terminal_ready) return SALTS_EBUSY;
  ++fixture->sends;
  fixture->terminal =
      (turbo_flow_transport_reply_terminal_t)TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_INIT;
  fixture->terminal.session = request->session;
  fixture->terminal.kind = TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_SENT;
  fixture->terminal.data_size = request->data_size;
  fixture->terminal.status = SALTS_OK;
  fixture->terminal.tag = request->tag;
  fixture->terminal_ready = 1;
  return SALTS_OK;
}

static int reply_take(void *ctx, turbo_flow_transport_reply_terminal_t *terminal) {
  reply_fixture_t *fixture = (reply_fixture_t *)ctx;
  if (!fixture || !terminal) return SALTS_EINVAL;
  if (!fixture->terminal_ready) return SALTS_ENOENT;
  *terminal = fixture->terminal;
  fixture->terminal_ready = 0;
  return SALTS_OK;
}

static int register_source(turbo_flow_t *flow, reply_fixture_t *fixture) {
  turbo_flow_adapter_ops_t adapter = {0};
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_transport_reply_provider_ops_t reply =
      TURBO_FLOW_TRANSPORT_REPLY_PROVIDER_OPS_INIT;
  int rc;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
  schema.direction = TURBO_FLOW_ADAPTER_INPUT;
  rc = turbo_flow_register_adapter_ex(flow, "source.reply", &adapter, fixture, &schema);
  if (rc != SALTS_OK) return rc;
  reply.capture = reply_capture;
  reply.send = reply_send;
  reply.take_terminal = reply_take;
  return turbo_flow_register_adapter_transport_reply(flow, "source.reply", &reply, fixture);
}

spec("Turbo Flow transport reply capability") {
  it("keeps capture, admission and terminal as separate generation-fenced facts") {
    turbo_flow_t *flow = turbo_flow_create();
    reply_fixture_t fixture = {0};
    turbo_flow_msg_t message;
    turbo_flow_transport_reply_session_t session = TURBO_FLOW_TRANSPORT_REPLY_SESSION_INIT;
    turbo_flow_transport_reply_request_t request = TURBO_FLOW_TRANSPORT_REPLY_REQUEST_INIT;
    turbo_flow_transport_reply_terminal_t terminal = TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_INIT;
    const char payload[] = "ack";

    check_not_null(flow);
    turbo_flow_msg_init(&message);
    message.id = 7u;
    check_equal(register_source(flow, &fixture), SALTS_OK);

    check_equal(turbo_flow_transport_reply_capture(flow, "source.reply", &message, &session),
                SALTS_OK);
    check_equal(session.token_size, sizeof(uint64_t));
    check_equal(fixture.sends, (size_t)0u);

    request.session = session;
    request.data = payload;
    request.data_size = sizeof(payload) - 1u;
    request.tag = 99u;
    check_equal(turbo_flow_transport_reply_send(flow, "source.reply", &request), SALTS_OK);
    check_equal(fixture.sends, (size_t)1u);

    check_equal(turbo_flow_transport_reply_take_terminal(flow, "source.reply", &terminal),
                SALTS_OK);
    check_equal(terminal.kind, TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_SENT);
    check_equal(terminal.data_size, sizeof(payload) - 1u);
    check_equal(terminal.status, SALTS_OK);
    check_equal(terminal.tag, UINT64_C(99));
    check_equal(terminal.session.token_size, session.token_size);
    check_equal(memcmp(terminal.session.token, session.token, session.token_size), 0);
    terminal = (turbo_flow_transport_reply_terminal_t)TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_INIT;
    check_equal(turbo_flow_transport_reply_take_terminal(flow, "source.reply", &terminal),
                SALTS_ENOENT);

    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
  }

  it("rejects malformed providers and non-source attachment") {
    turbo_flow_t *flow = turbo_flow_create();
    reply_fixture_t fixture = {0};
    turbo_flow_adapter_ops_t adapter = {0};
    turbo_flow_adapter_schema_t schema = {0};
    turbo_flow_transport_reply_provider_ops_t reply =
        TURBO_FLOW_TRANSPORT_REPLY_PROVIDER_OPS_INIT;

    check_not_null(flow);
    reply.capture = reply_capture;
    reply.send = reply_send;
    reply.take_terminal = reply_take;
    check_equal(turbo_flow_register_adapter_transport_reply(flow, "missing", &reply, &fixture),
                SALTS_ENOENT);

    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_equal(turbo_flow_register_adapter_ex(flow, "sink.only", &adapter, &fixture, &schema),
                SALTS_OK);
    check_equal(turbo_flow_register_adapter_transport_reply(flow, "sink.only", &reply, &fixture),
                SALTS_EINVAL);

    reply.take_terminal = NULL;
    check_equal(turbo_flow_register_adapter_transport_reply(flow, "sink.only", &reply, &fixture),
                SALTS_EINVAL);
    turbo_flow_destroy(flow);
  }
}
