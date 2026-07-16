#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_protocol.h"

typedef struct protocol_owner_probe_s {
  int called;
  int result;
  turbo_flow_protocol_settlement_request_t observed;
} protocol_owner_probe_t;

typedef struct protocol_route_owner_probe_s {
  int called;
  turbo_flow_protocol_route_t route;
  turbo_flow_protocol_settlement_request_t request;
} protocol_route_owner_probe_t;

static turbo_flow_protocol_message_t mqtt_message(uint32_t version, uint32_t qos,
                                                  uint32_t packet_id) {
  turbo_flow_protocol_message_t message = TURBO_FLOW_PROTOCOL_MESSAGE_INIT;
  message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  message.protocol_version = version;
  message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
  message.qos = qos;
  message.packet_id = packet_id;
  message.session_generation = 7u;
  return message;
}

static turbo_flow_protocol_settlement_request_t
settlement_request(turbo_flow_protocol_message_t message,
                   turbo_flow_protocol_settlement_point_t point) {
  turbo_flow_protocol_settlement_request_t request = TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
  request.message = message;
  request.point = point;
  request.message_id = 41u;
  request.attempt = 1u;
  return request;
}

static int protocol_owner_settle(void *ctx,
                                 const turbo_flow_protocol_settlement_request_t *request) {
  protocol_owner_probe_t *probe = (protocol_owner_probe_t *)ctx;
  probe->called += 1;
  probe->observed = *request;
  return probe->result;
}

static int protocol_route_owner_settle(
    void *ctx, const turbo_flow_protocol_route_t *route,
    const turbo_flow_protocol_settlement_request_t *request) {
  protocol_route_owner_probe_t *probe = (protocol_route_owner_probe_t *)ctx;
  probe->called += 1;
  probe->route = *route;
  probe->request = *request;
  return TURBO_OK;
}

static int protocol_accept_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)ctx;
  return turbo_flow_msg_complete_protocol_settlement(msg,
                                                     TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED);
}

suite("Turbo Flow Protocol Contract") {
  group("FMQ pattern validation") {
    it("owns FMQ pattern validation and supported peer pairings") {
      check_int_eq(turbo_flow_fmq_pattern_validate(TURBO_FLOW_FMQ_PUB), TURBO_OK);
      check_int_eq(turbo_flow_fmq_pattern_validate(TURBO_FLOW_FMQ_XSUB), TURBO_OK);
      check_int_eq(turbo_flow_fmq_pattern_validate((turbo_flow_fmq_pattern_t)0), TURBO_EINVAL);
      check_int_eq(turbo_flow_fmq_pattern_validate((turbo_flow_fmq_pattern_t)12), TURBO_EINVAL);
      check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_SUB));
      check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_XPUB, TURBO_FLOW_FMQ_XSUB));
      check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUSH, TURBO_FLOW_FMQ_PULL));
      check_true(
          turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_ROUTER, TURBO_FLOW_FMQ_DEALER));
      check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PAIR, TURBO_FLOW_FMQ_PAIR));
      check_true(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_REQ, TURBO_FLOW_FMQ_REP));
      check_false(turbo_flow_fmq_patterns_compatible(TURBO_FLOW_FMQ_PUB, TURBO_FLOW_FMQ_PULL));
      check_false(turbo_flow_fmq_patterns_compatible((turbo_flow_fmq_pattern_t)0,
                                                     TURBO_FLOW_FMQ_SUB));
    }
  }

  group("Protocol-neutral pattern core") {
    it("maps compatible roles without owning a wire protocol") {
      check_int_eq(turbo_flow_pattern_role_validate(TURBO_FLOW_PATTERN_PUBLISH), TURBO_OK);
      check_true(turbo_flow_pattern_roles_compatible(TURBO_FLOW_PATTERN_PUBLISH,
                                                      TURBO_FLOW_PATTERN_SUBSCRIBE));
      check_true(turbo_flow_pattern_roles_compatible(TURBO_FLOW_PATTERN_EXTENDED_PUBLISH,
                                                      TURBO_FLOW_PATTERN_EXTENDED_SUBSCRIBE));
      check_true(turbo_flow_pattern_roles_compatible(TURBO_FLOW_PATTERN_PUSH,
                                                      TURBO_FLOW_PATTERN_PULL));
      check_true(turbo_flow_pattern_roles_compatible(TURBO_FLOW_PATTERN_ROUTE,
                                                      TURBO_FLOW_PATTERN_DEAL));
      check_true(turbo_flow_pattern_roles_compatible(TURBO_FLOW_PATTERN_REQUEST,
                                                      TURBO_FLOW_PATTERN_REPLY));
      check_false(turbo_flow_pattern_roles_compatible(TURBO_FLOW_PATTERN_PUBLISH,
                                                       TURBO_FLOW_PATTERN_PULL));
      check_int_eq(turbo_flow_pattern_role_validate((turbo_flow_pattern_role_t)0),
                   TURBO_EINVAL);
    }

    it("iterates fan-out and round-robin candidates without owning them") {
      turbo_flow_pattern_selector_t selector;
      turbo_flow_pattern_selection_iterator_t iterator =
          TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
      size_t index = SIZE_MAX;

      check_int_eq(turbo_flow_pattern_selector_init(&selector), TURBO_OK);
      check_int_eq(turbo_flow_pattern_selection_begin(
                       &selector, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, 3u, &iterator),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_selection_next(&iterator, &index), TURBO_OK);
      check_size_eq(index, 0u);
      check_int_eq(turbo_flow_pattern_selection_next(&iterator, &index), TURBO_OK);
      check_size_eq(index, 1u);
      check_int_eq(turbo_flow_pattern_selection_next(&iterator, &index), TURBO_OK);
      check_size_eq(index, 2u);
      check_int_eq(turbo_flow_pattern_selection_next(&iterator, &index), TURBO_ENOENT);

      iterator = (turbo_flow_pattern_selection_iterator_t)
          TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
      check_int_eq(turbo_flow_pattern_selection_begin(
                       &selector, TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN, 3u, &iterator),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_selection_next(&iterator, &index), TURBO_OK);
      check_size_eq(index, 0u);
      iterator = (turbo_flow_pattern_selection_iterator_t)
          TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
      check_int_eq(turbo_flow_pattern_selection_begin(
                       &selector, TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN, 3u, &iterator),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_selection_next(&iterator, &index), TURBO_OK);
      check_size_eq(index, 1u);
      check_int_eq(turbo_flow_pattern_selection_begin(
                       &selector, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, 0u, &iterator),
                   TURBO_ENOENT);
    }

    it("matches only nonzero generation-fenced routes") {
      turbo_flow_pattern_route_t requested = TURBO_FLOW_PATTERN_ROUTE_INIT;
      turbo_flow_pattern_route_t candidate = TURBO_FLOW_PATTERN_ROUTE_INIT;
      requested.route_id = candidate.route_id = 17u;
      requested.generation = candidate.generation = 4u;

      check_int_eq(turbo_flow_pattern_route_validate(&requested), TURBO_OK);
      check_true(turbo_flow_pattern_routes_match(&requested, &candidate));
      candidate.generation += 1u;
      check_false(turbo_flow_pattern_routes_match(&requested, &candidate));
      candidate = requested;
      candidate.route_id = 0u;
      check_false(turbo_flow_pattern_routes_match(&requested, &candidate));
    }

    it("owns one synchronous correlation until ready or session reset") {
      turbo_flow_pattern_exchange_t exchange;
      turbo_flow_pattern_exchange_state_t state = TURBO_FLOW_PATTERN_EXCHANGE_RESETTING;
      uint64_t correlation_id = UINT64_MAX;

      check_int_eq(turbo_flow_pattern_exchange_init(&exchange), TURBO_OK);
      check_int_eq(turbo_flow_pattern_exchange_snapshot(&exchange, &state, &correlation_id),
                   TURBO_OK);
      check_int_eq(state, TURBO_FLOW_PATTERN_EXCHANGE_READY);
      check_uint_eq(correlation_id, 0u);
      check_int_eq(turbo_flow_pattern_exchange_begin(
                       &exchange, TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY, 41u),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_exchange_begin(
                       &exchange, TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY, 42u),
                   TURBO_EBUSY);
      check_int_eq(turbo_flow_pattern_exchange_match(
                       &exchange, TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY, 42u),
                   TURBO_EPROTO);
      check_int_eq(turbo_flow_pattern_exchange_finish(
                       &exchange, TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY, 41u,
                       TURBO_FLOW_PATTERN_EXCHANGE_RESETTING),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_exchange_snapshot(&exchange, &state, &correlation_id),
                   TURBO_OK);
      check_int_eq(state, TURBO_FLOW_PATTERN_EXCHANGE_RESETTING);
      check_uint_eq(correlation_id, 41u);
      check_int_eq(turbo_flow_pattern_exchange_reset(&exchange), TURBO_OK);
      check_int_eq(turbo_flow_pattern_exchange_begin(
                       &exchange, TURBO_FLOW_PATTERN_EXCHANGE_PROCESSING_REQUEST, 51u),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_exchange_finish(
                       &exchange, TURBO_FLOW_PATTERN_EXCHANGE_PROCESSING_REQUEST, 51u,
                       TURBO_FLOW_PATTERN_EXCHANGE_READY),
                   TURBO_OK);
      check_int_eq(turbo_flow_pattern_exchange_snapshot(&exchange, &state, &correlation_id),
                   TURBO_OK);
      check_int_eq(state, TURBO_FLOW_PATTERN_EXCHANGE_READY);
      check_uint_eq(correlation_id, 0u);
    }
  }

  group("MQTT message validation") {
    it("accepts MQTT 3.1, 3.1.1 and 5.0 publish metadata") {
      turbo_flow_protocol_message_t mqtt31 = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_3_1, TURBO_FLOW_PROTOCOL_QOS_1, 8u);
      turbo_flow_protocol_message_t mqtt311 = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_3_1_1, TURBO_FLOW_PROTOCOL_QOS_1, 9u);
      turbo_flow_protocol_message_t mqtt5 = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_5_0, TURBO_FLOW_PROTOCOL_QOS_2, 10u);

      check_int_eq(turbo_flow_protocol_message_validate(&mqtt31), TURBO_OK);
      check_int_eq(turbo_flow_protocol_message_validate(&mqtt311), TURBO_OK);
      check_int_eq(turbo_flow_protocol_message_validate(&mqtt5), TURBO_OK);
    }

    it("requires packet id zero for QoS 0") {
      turbo_flow_protocol_message_t message = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_5_0, TURBO_FLOW_PROTOCOL_QOS_0, 1u);

      check_int_eq(turbo_flow_protocol_message_validate(&message), TURBO_EPROTO);
      message.packet_id = 0u;
      check_int_eq(turbo_flow_protocol_message_validate(&message), TURBO_OK);
    }

    it("requires a nonzero packet id for QoS 1 and QoS 2") {
      turbo_flow_protocol_message_t qos1 = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_5_0, TURBO_FLOW_PROTOCOL_QOS_1, 0u);
      turbo_flow_protocol_message_t qos2 = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_5_0, TURBO_FLOW_PROTOCOL_QOS_2, 0u);

      check_int_eq(turbo_flow_protocol_message_validate(&qos1), TURBO_EPROTO);
      check_int_eq(turbo_flow_protocol_message_validate(&qos2), TURBO_EPROTO);
      qos1.packet_id = 1u;
      qos2.packet_id = 2u;
      check_int_eq(turbo_flow_protocol_message_validate(&qos1), TURBO_OK);
      check_int_eq(turbo_flow_protocol_message_validate(&qos2), TURBO_OK);
    }

    it("rejects an absent session generation") {
      turbo_flow_protocol_message_t message = mqtt_message(
          TURBO_FLOW_MQTT_PROTOCOL_5_0, TURBO_FLOW_PROTOCOL_QOS_1, 1u);
      message.session_generation = 0u;

      check_int_eq(turbo_flow_protocol_message_validate(&message), TURBO_EINVAL);
    }
  }

  group("Owner settlement") {
    it("expresses the current received-time MQTT acknowledgement policy") {
      turbo_flow_protocol_owner_ops_t owner = TURBO_FLOW_PROTOCOL_OWNER_OPS_INIT;
      turbo_flow_protocol_settlement_policy_t policy =
          TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT;
      turbo_flow_protocol_settlement_request_t request =
          settlement_request(mqtt_message(TURBO_FLOW_MQTT_PROTOCOL_5_0,
                                          TURBO_FLOW_PROTOCOL_QOS_1, 23u),
                             TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED);
      protocol_owner_probe_t probe = {0};
      owner.settle = protocol_owner_settle;

      check_int_eq(turbo_flow_protocol_owner_settle(&owner, &probe, &policy, &request), TURBO_OK);
      check_int_eq(probe.called, 1);
      check_uint_eq(probe.observed.message.packet_id, 23u);
      check_uint_eq(probe.observed.point, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED);
    }

    it("does not call a durable owner before the durable point") {
      turbo_flow_protocol_owner_ops_t owner = TURBO_FLOW_PROTOCOL_OWNER_OPS_INIT;
      turbo_flow_protocol_settlement_policy_t policy =
          TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT;
      turbo_flow_protocol_settlement_request_t request =
          settlement_request(mqtt_message(TURBO_FLOW_MQTT_PROTOCOL_5_0,
                                          TURBO_FLOW_PROTOCOL_QOS_2, 24u),
                             TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED);
      protocol_owner_probe_t probe = {0};
      owner.settle = protocol_owner_settle;
      policy.qos2 = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;

      check_int_eq(turbo_flow_protocol_owner_settle(&owner, &probe, &policy, &request),
                   TURBO_EBUSY);
      check_int_eq(probe.called, 0);
      request.point = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
      check_int_eq(turbo_flow_protocol_owner_settle(&owner, &probe, &policy, &request), TURBO_OK);
      check_int_eq(probe.called, 1);
      check_uint_eq(probe.observed.point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    }

    it("never acknowledges without a protocol owner callback") {
      turbo_flow_protocol_owner_ops_t owner = TURBO_FLOW_PROTOCOL_OWNER_OPS_INIT;
      turbo_flow_protocol_settlement_policy_t policy =
          TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT;
      turbo_flow_protocol_settlement_request_t request =
          settlement_request(mqtt_message(TURBO_FLOW_MQTT_PROTOCOL_3_1_1,
                                          TURBO_FLOW_PROTOCOL_QOS_1, 25u),
                             TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED);
      protocol_owner_probe_t probe = {0};

      check_int_eq(turbo_flow_protocol_owner_settle(&owner, &probe, &policy, &request),
                   TURBO_EINVAL);
      check_int_eq(probe.called, 0);
    }

    it("dispatches by protocol owner instance and rejects a stale generation") {
      turbo_flow_protocol_route_owner_ops_t ops = TURBO_FLOW_PROTOCOL_ROUTE_OWNER_OPS_INIT;
      turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
      turbo_flow_protocol_settlement_request_t request =
          settlement_request(mqtt_message(TURBO_FLOW_MQTT_PROTOCOL_5_0,
                                          TURBO_FLOW_PROTOCOL_QOS_1, 31u),
                             TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED);
      protocol_route_owner_probe_t probe = {0};
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      ops.settle = protocol_route_owner_settle;
      route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
      route.owner_instance_id = 91u;
      route.session_id = 17u;
      route.session_generation = request.message.session_generation;

      check_int_eq(turbo_flow_register_protocol_route_owner(
                       flow, TURBO_FLOW_PROTOCOL_MQTT, route.owner_instance_id, &ops, &probe),
                   TURBO_OK);
      check_int_eq(turbo_flow_protocol_route_settle(flow, &route, &request), TURBO_OK);
      check_int_eq(probe.called, 1);
      check_uint_eq(probe.route.session_id, route.session_id);
      check_uint_eq(probe.request.point, TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED);
      request.message.session_generation += 1u;
      check_int_eq(turbo_flow_protocol_route_settle(flow, &route, &request), TURBO_EPROTO);
      check_int_eq(probe.called, 1);
      check_int_eq(turbo_flow_unregister_protocol_route_owner(
                       flow, TURBO_FLOW_PROTOCOL_MQTT, route.owner_instance_id),
                   TURBO_OK);
      check_int_eq(turbo_flow_protocol_route_settle(flow, &route, &probe.request), TURBO_ENOENT);
      turbo_flow_destroy(flow);
    }

    it("copies a settlement envelope and reports the exact graph boundary") {
      static const char graph[] = "source input\n"
                                  "stage accept\n"
                                  "stage main {\n"
                                  "  input -> accept\n"
                                  "}\n";
      turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
      turbo_flow_protocol_settlement_envelope_t envelope =
          TURBO_FLOW_PROTOCOL_SETTLEMENT_ENVELOPE_INIT;
      turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
      turbo_flow_msg_t msg;
      turbo_flow_msg_t clone;
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
      route.owner_instance_id = 92u;
      route.session_id = 18u;
      route.session_generation = 7u;
      envelope.message = mqtt_message(TURBO_FLOW_MQTT_PROTOCOL_5_0,
                                      TURBO_FLOW_PROTOCOL_QOS_1, 32u);
      envelope.requested_point = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
      turbo_flow_msg_init(&msg);
      turbo_flow_msg_init(&clone);
      check_int_eq(turbo_flow_msg_set_protocol_route(&msg, &route), TURBO_OK);
      check_int_eq(turbo_flow_msg_set_protocol_settlement(&msg, &envelope), TURBO_OK);
      check_int_eq(turbo_flow_msg_clone(&clone, &msg), TURBO_OK);
      check_not_null(turbo_flow_msg_protocol_settlement(&clone));
      check_uint_eq(turbo_flow_msg_protocol_settlement(&clone)->settled_point, 0u);
      check_int_eq(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "accept", protocol_accept_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      check_int_eq(turbo_flow_publish_ex(flow, "input", &clone, &result), TURBO_OK);
      check_int_eq(result.status, TURBO_OK);
      check_uint_eq(result.protocol_settlement, TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED);
      check_uint_eq(turbo_flow_msg_protocol_settlement(&clone)->settled_point, 0u);
      turbo_flow_msg_clear_protocol_route(&clone);
      check_null(turbo_flow_msg_protocol_route(&clone));
      check_null(turbo_flow_msg_protocol_settlement(&clone));
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_msg_cleanup(&clone);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_destroy(flow);
    }
  }
}
