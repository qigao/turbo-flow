#include "flowie_ingress_internal.h"
#include "flowie_rule_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <stdio.h>
#include <string.h>

typedef struct flowie_ingress_capture_s {
  size_t calls;
  uint32_t types[4];
  uint32_t flags[4];
  int buffer_backed[4];
  int owned_payload[4];
  int projection_bound[4];
  tstr_t packets[4];
  tstr_t topics[4];
  int result;
} flowie_ingress_capture_t;

typedef struct flowie_ingress_completion_probe_s {
  flowie_ingress_capture_t *capture;
  size_t calls;
  size_t captured_before_callback;
  int status;
} flowie_ingress_completion_probe_t;

static int flowie_ingress_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  flowie_ingress_capture_t *capture = (flowie_ingress_capture_t *)ctx;
  size_t index = capture->calls++;
  if (index < 4u) {
    capture->types[index] = msg->type;
    capture->flags[index] = msg->flags;
    capture->buffer_backed[index] = msg->buffer != NULL;
    capture->owned_payload[index] = msg->owned_payload != NULL;
    capture->projection_bound[index] =
        turbo_flow_msg_content_state(msg) == TURBO_FLOW_CONTENT_SCHEMA_BOUND;
    capture->packets[index] = tstr_new_len(msg->payload.data, msg->payload.len);
    if (!capture->packets[index]) return TURBO_ENOMEM;
    if (msg->type == FLOWIE_MQTT_PACKET_PUBLISH) {
      const turbo_flow_expr_value_t *values = NULL;
      size_t value_count = 0u;
      int rc = flowie_mqtt_rule_facts_provider(msg, flowie_mqtt_rule_schema(), &values,
                                               &value_count, NULL);
      if (rc != TURBO_OK) return rc;
      if (value_count == 0u || values[0].type != TURBO_FLOW_EXPR_TYPE_STRING) return TURBO_EPROTO;
      capture->topics[index] = tstr_from_v(values[0].as.string);
      if (!capture->topics[index]) return TURBO_ENOMEM;
    }
  }
  return capture->result;
}

static int flowie_ingress_graph_dispatch(void *ctx, turbo_flow_msg_t *message,
                                         turbo_flow_publish_result_t *result) {
  return turbo_flow_publish_ex((turbo_flow_t *)ctx, "mqtt_in", message, result);
}

static void flowie_ingress_capture_cleanup(flowie_ingress_capture_t *capture) {
  for (size_t i = 0u; i < 4u; ++i) {
    tstr_freep(&capture->packets[i]);
    tstr_freep(&capture->topics[i]);
  }
}

static int flowie_ingress_completion(void *ctx, flowie_ingress_t *ingress,
                                     const turbo_flow_msg_t *message,
                                     const turbo_flow_publish_result_t *result) {
  flowie_ingress_completion_probe_t *probe = (flowie_ingress_completion_probe_t *)ctx;
  (void)ingress;
  if (!probe || !message || !result) return TURBO_EINVAL;
  probe->calls += 1u;
  probe->captured_before_callback = probe->capture->calls;
  probe->status = result->status;
  return result->status;
}

static turbo_flow_t *flowie_ingress_flow(const char *stage_declaration,
                                         flowie_ingress_capture_t *capture) {
  char graph[256];
  turbo_flow_t *flow = turbo_flow_create();
  int length;
  if (!flow) return NULL;
  length = snprintf(graph, sizeof(graph),
                    "source mqtt_in\nstage capture%s\nstage main {\n"
                    "  mqtt_in -> capture\n}\n",
                    stage_declaration);
  if (length < 0 || (size_t)length >= sizeof(graph) ||
      turbo_flow_register_stage_ex(flow, "capture", flowie_ingress_capture_stage, capture, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, (size_t)length) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK || turbo_flow_start(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static flowie_ingress_t *flowie_ingress_for(turbo_flow_t *flow, size_t max_packet_size) {
  flowie_ingress_config_t config = FLOWIE_INGRESS_CONFIG_INIT;
  config.dispatch = flowie_ingress_graph_dispatch;
  config.dispatch_ctx = flow;
  config.max_packet_size = max_packet_size;
  return flowie_ingress_create(&config);
}

spec("flowie MQTT connection ingress") {
  it("materializes versioned MQTT PUBLISH facts from serializable message metadata") {
    static const uint8_t mqtt5_publish[] = {0x31u, 0x06u, 0x00u, 0x01u, 't', 0x00u, 'x', 'y'};
    static const uint8_t mqtt311_publish[] = {0x3au, 0x06u, 0x00u, 0x01u, 'v', 0x00u, 0x09u, 'z'};
    const turbo_flow_expr_schema_t *schema = flowie_mqtt_rule_schema();
    const turbo_flow_expr_value_t *values = NULL;
    turbo_flow_expr_schema_field_t missing_field = {"mqtt.missing", TURBO_FLOW_EXPR_TYPE_STRING,
                                                    99u};
    turbo_flow_expr_schema_t missing_schema = {&missing_field, 1u};
    turbo_flow_msg_t message;
    size_t value_count = 0u;
    check_not_null(schema);
    check_size_eq(schema->field_count, 15u);

    turbo_flow_msg_init(&message);
    message.payload = tstr_v_from_buf((const char *)mqtt5_publish, sizeof(mqtt5_publish));
    check_int_eq(flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, mqtt5_publish[0] & 0x0fu,
                                                  &message.flags),
                 TURBO_OK);
    check_int_eq(flowie_mqtt_rule_facts_provider(&message, schema, &values, &value_count, NULL),
                 TURBO_OK);
    check_size_eq(value_count, 15u);
    check_int_eq(values[0].type, TURBO_FLOW_EXPR_TYPE_STRING);
    check_mem_eq(values[0].as.string.data, "t", 1u);
    check_mem_eq(values[1].as.string.data, "xy", 2u);
    check_int_eq(values[2].as.i64, 2);
    check_int_eq(values[3].as.i64, 0);
    check_true(values[4].as.boolean);
    check_false(values[5].as.boolean);
    check_int_eq(values[6].as.i64, 0);
    check_int_eq(values[7].as.i64, FLOWIE_MQTT_VERSION_5);
    for (size_t i = 8u; i < 13u; ++i)
      check_int_eq(values[i].type, TURBO_FLOW_EXPR_TYPE_NULL);
    check_int_eq(values[13].as.i64, 0);
    check_false(values[14].as.boolean);

    message.payload = tstr_v_from_buf((const char *)mqtt311_publish, sizeof(mqtt311_publish));
    check_int_eq(flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_3_1_1,
                                                  mqtt311_publish[0] & 0x0fu, &message.flags),
                 TURBO_OK);
    check_int_eq(flowie_mqtt_rule_facts_provider(&message, schema, &values, &value_count, NULL),
                 TURBO_OK);
    check_mem_eq(values[0].as.string.data, "v", 1u);
    check_mem_eq(values[1].as.string.data, "z", 1u);
    check_int_eq(values[2].as.i64, 1);
    check_int_eq(values[3].as.i64, 1);
    check_false(values[4].as.boolean);
    check_true(values[5].as.boolean);
    check_int_eq(values[6].as.i64, 9);
    check_int_eq(values[7].as.i64, FLOWIE_MQTT_VERSION_3_1_1);
    for (size_t i = 8u; i < 13u; ++i)
      check_int_eq(values[i].type, TURBO_FLOW_EXPR_TYPE_NULL);
    check_int_eq(values[13].as.i64, 0);
    check_false(values[14].as.boolean);

    values = NULL;
    value_count = 0u;
    check_int_eq(
        flowie_mqtt_rule_facts_provider(&message, &missing_schema, &values, &value_count, NULL),
        TURBO_ENOENT);
    check_null(values);
    check_size_eq(value_count, 0u);
    message.flags = 0u;
    check_int_eq(flowie_mqtt_rule_facts_provider(&message, schema, &values, &value_count, NULL),
                 TURBO_EPROTO);
    check_int_eq(
        flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_UNSPECIFIED, 0u, &message.flags),
        TURBO_EINVAL);
    check_int_eq(flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, 0x10u, &message.flags),
                 TURBO_EINVAL);
    turbo_flow_msg_cleanup(&message);
  }

  it("materializes optional MQTT 5 properties into bitmap-backed rule facts") {
    static const uint8_t properties[] = {FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR,
                                         0x01u,
                                         FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL,
                                         0x00u,
                                         0x00u,
                                         0x00u,
                                         0x3cu,
                                         FLOWIE_MQTT_PROPERTY_CONTENT_TYPE,
                                         0x00u,
                                         0x10u,
                                         'a',
                                         'p',
                                         'p',
                                         'l',
                                         'i',
                                         'c',
                                         'a',
                                         't',
                                         'i',
                                         'o',
                                         'n',
                                         '/',
                                         'j',
                                         's',
                                         'o',
                                         'n',
                                         FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC,
                                         0x00u,
                                         0x0cu,
                                         'r',
                                         'e',
                                         'p',
                                         'l',
                                         'y',
                                         '/',
                                         'r',
                                         'e',
                                         's',
                                         'u',
                                         'l',
                                         't',
                                         FLOWIE_MQTT_PROPERTY_CORRELATION_DATA,
                                         0x00u,
                                         0x04u,
                                         0x00u,
                                         0xffu,
                                         0x01u,
                                         0x02u,
                                         FLOWIE_MQTT_PROPERTY_USER_PROPERTY,
                                         0x00u,
                                         0x01u,
                                         'a',
                                         0x00u,
                                         0x01u,
                                         '1',
                                         FLOWIE_MQTT_PROPERTY_USER_PROPERTY,
                                         0x00u,
                                         0x01u,
                                         'b',
                                         0x00u,
                                         0x01u,
                                         '2'};
    static const uint8_t correlation[] = {0x00u, 0xffu, 0x01u, 0x02u};
    static const uint8_t topic[] = "properties/input";
    static const uint8_t payload[] = "body";
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    const turbo_flow_expr_value_t *values = NULL;
    turbo_flow_msg_t message;
    uint8_t wire[128];
    size_t value_count = 0u;
    size_t wire_size = 0u;

    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
    publish.properties = (flowie_mqtt_span_t){properties, sizeof(properties)};
    publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, wire, sizeof(wire), &wire_size),
                 FLOWIE_MQTT_PARSE_OK);

    turbo_flow_msg_init(&message);
    message.type = FLOWIE_MQTT_PACKET_PUBLISH;
    message.payload = tstr_v_from_buf((const char *)wire, wire_size);
    check_int_eq(
        flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, wire[0] & 0x0fu, &message.flags),
        TURBO_OK);
    message.flags |= FLOWIE_MQTT_MESSAGE_BROKER_WILL;
    check_int_eq(flowie_mqtt_rule_facts_provider(&message, flowie_mqtt_rule_schema(), &values,
                                                 &value_count, NULL),
                 TURBO_OK);
    check_size_eq(value_count, 15u);
    check_int_eq(values[8].type, TURBO_FLOW_EXPR_TYPE_I64);
    check_int_eq(values[8].as.i64, 1);
    check_int_eq(values[9].as.i64, 60);
    check_int_eq(values[10].type, TURBO_FLOW_EXPR_TYPE_STRING);
    check_mem_eq(values[10].as.string.data, "application/json", 16u);
    check_mem_eq(values[11].as.string.data, "reply/result", 12u);
    check_size_eq(values[12].as.string.len, sizeof(correlation));
    check_mem_eq(values[12].as.string.data, correlation, sizeof(correlation));
    check_int_eq(values[13].as.i64, 2);
    check_true(values[14].as.boolean);
    turbo_flow_msg_cleanup(&message);
  }

  it("binds one buffer-owned projection and deep-clones branch-local field state") {
    uint8_t source_packet[] = {0x31u, 0x06u, 0x00u, 0x01u, 't', 0x00u, 'x', 'y'};
    static const uint8_t replacement_packet[] = {0x30u, 0x05u, 0x00u, 0x01u, 'r', 0x00u, 'z'};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    const turbo_flow_data_schema_t *projection_schema = NULL;
    const turbo_flow_expr_value_t *values = NULL;
    const void *source_projection;
    const void *clone_projection;
    turbo_flow_msg_t message;
    turbo_flow_msg_t clone;
    size_t consumed = 0u;
    size_t value_count = 0u;

    options.version = FLOWIE_MQTT_VERSION_5;
    options.max_packet_size = sizeof(source_packet);
    check_int_eq(flowie_mqtt_packet_parse(source_packet, sizeof(source_packet), &options, &packet,
                                          &consumed, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(source_packet));
    turbo_flow_msg_init(&message);
    message.type = FLOWIE_MQTT_PACKET_PUBLISH;
    check_int_eq(flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, source_packet[0] & 0x0fu,
                                                  &message.flags),
                 TURBO_OK);
    message.buffer = mem_get_buffer(mem_global(), sizeof(source_packet));
    check_not_null(message.buffer);
    memcpy(mem_buffer_data(message.buffer), source_packet, sizeof(source_packet));
    mem_set_used(message.buffer, sizeof(source_packet));
    message.payload = tstr_v_from_buf(mem_buffer_data(message.buffer), sizeof(source_packet));

    check_int_eq(flowie_mqtt_rule_bind_projection(&message, &packet), TURBO_OK);
    source_projection = turbo_flow_msg_projection(&message, &projection_schema);
    check_not_null(source_projection);
    check_not_null(projection_schema);
    check_str_eq(projection_schema->schema_name, "flowie.mqtt.publish");
    memset(source_packet, 0, sizeof(source_packet));
    check_int_eq(flowie_mqtt_rule_facts_provider(&message, flowie_mqtt_rule_schema(), &values,
                                                 &value_count, NULL),
                 TURBO_OK);
    check_mem_eq(values[0].as.string.data, "t", 1u);
    check_mem_eq(values[1].as.string.data, "xy", 2u);

    check_int_eq(turbo_flow_msg_clone(&clone, &message), TURBO_OK);
    clone_projection = turbo_flow_msg_projection(&clone, NULL);
    check_not_null(clone_projection);
    check_true(clone_projection != source_projection);
    turbo_flow_msg_cleanup(&message);
    values = NULL;
    value_count = 0u;
    check_int_eq(flowie_mqtt_rule_facts_provider(&clone, flowie_mqtt_rule_schema(), &values,
                                                 &value_count, NULL),
                 TURBO_OK);
    check_mem_eq(values[0].as.string.data, "t", 1u);
    check_mem_eq(values[1].as.string.data, "xy", 2u);

    clone.payload = tstr_v_from_buf((const char *)replacement_packet, sizeof(replacement_packet));
    check_int_eq(flowie_mqtt_rule_facts_provider(&clone, flowie_mqtt_rule_schema(), &values,
                                                 &value_count, NULL),
                 TURBO_EPROTO);
    turbo_flow_msg_clear_projection(&clone);
    check_int_eq(flowie_mqtt_rule_facts_provider(&clone, flowie_mqtt_rule_schema(), &values,
                                                 &value_count, NULL),
                 TURBO_OK);
    check_mem_eq(values[0].as.string.data, "r", 1u);
    check_mem_eq(values[1].as.string.data, "z", 1u);
    turbo_flow_msg_cleanup(&clone);
  }

  it("keeps a half packet buffered and publishes one buffer-backed packet when complete") {
    static const uint8_t connect[] = {0x10, 0x17, 0x00, 0x04, 'M',  'Q',  'T',  'T', 0x05,
                                      0x02, 0x00, 0x3c, 0x07, 0x15, 0x00, 0x04, 'n', 'o',
                                      'n',  'e',  0x00, 0x03, 'c',  'l',  'i'};
    static const uint8_t packet[] = {0x30, 0x07, 0x00, 0x01, 'a', 0x00, 'o', 'k', '!'};
    flowie_ingress_capture_t capture = {0};
    turbo_flow_t *flow = flowie_ingress_flow("", &capture);
    flowie_ingress_t *ingress = flowie_ingress_for(flow, 32u);
    size_t published = 99u;
    uint32_t expected_flags = 0u;

    check_not_null(flow);
    check_not_null(ingress);
    check_int_eq(flowie_ingress_feed(ingress, connect, sizeof(connect), &published), TURBO_OK);
    check_size_eq(published, 1u);
    check_int_eq(flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, 0u, &expected_flags),
                 TURBO_OK);
    check_uint_eq(capture.flags[0], expected_flags);
    check_int_eq(flowie_ingress_feed(ingress, packet, 3u, &published), TURBO_OK);
    check_size_eq(published, 0u);
    check_size_eq(capture.calls, 1u);
    check_size_eq(flowie_ingress_buffered_bytes(ingress), 3u);
    check_int_eq(flowie_ingress_feed(ingress, packet + 3u, sizeof(packet) - 3u, &published),
                 TURBO_OK);
    check_size_eq(published, 1u);
    check_size_eq(capture.calls, 2u);
    check_uint_eq(capture.types[1], FLOWIE_MQTT_PACKET_PUBLISH);
    check_uint_eq(capture.flags[1], expected_flags);
    check_int_eq(capture.buffer_backed[1], 1);
    check_int_eq(capture.owned_payload[1], 0);
    check_int_eq(capture.projection_bound[1], 1);
    check_str_eq(capture.topics[1], "a");
    check_size_eq(flowie_ingress_buffered_bytes(ingress), 0u);
    check_size_eq(tstr_len(capture.packets[1]), sizeof(packet));
    check_mem_eq(capture.packets[1], packet, sizeof(packet));

    flowie_ingress_destroy(ingress);
    check_mem_eq(capture.packets[1], packet, sizeof(packet));
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_ingress_capture_cleanup(&capture);
  }

  it("publishes every complete packet from one sticky receive chunk") {
    static const uint8_t packets[] = {0x10, 0x17, 0x00, 0x04, 'M',  'Q',  'T',  'T',  0x05, 0x02,
                                      0x00, 0x3c, 0x07, 0x15, 0x00, 0x04, 'n',  'o',  'n',  'e',
                                      0x00, 0x03, 'c',  'l',  'i',  0xc0, 0x00, 0xc0, 0x00};
    flowie_ingress_capture_t capture = {0};
    turbo_flow_t *flow = flowie_ingress_flow("", &capture);
    flowie_ingress_t *ingress = flowie_ingress_for(flow, sizeof(packets));
    size_t published = 0u;

    check_not_null(flow);
    check_not_null(ingress);
    check_int_eq(flowie_ingress_feed(ingress, packets, sizeof(packets), &published), TURBO_OK);
    check_size_eq(published, 3u);
    check_size_eq(capture.calls, 3u);
    check_uint_eq(capture.types[0], FLOWIE_MQTT_PACKET_CONNECT);
    check_uint_eq(capture.types[1], FLOWIE_MQTT_PACKET_PINGREQ);
    check_uint_eq(capture.types[2], FLOWIE_MQTT_PACKET_PINGREQ);
    check_mem_eq(capture.packets[1], packets + sizeof(packets) - 4u, 2u);
    check_mem_eq(capture.packets[2], packets + sizeof(packets) - 2u, 2u);

    flowie_ingress_destroy(ingress);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_ingress_capture_cleanup(&capture);
  }

  it("uses the configured worker Disruptor after packet ownership transfer") {
    static const uint8_t packet[] = {0x10, 0x17, 0x00, 0x04, 'M',  'Q',  'T',  'T', 0x05,
                                     0x02, 0x00, 0x3c, 0x07, 0x15, 0x00, 0x04, 'n', 'o',
                                     'n',  'e',  0x00, 0x03, 'c',  'l',  'i'};
    flowie_ingress_capture_t capture = {0};
    turbo_flow_pool_snapshot_t pool = {0};
    turbo_flow_t *flow = flowie_ingress_flow(" worker 1 capacity 8", &capture);
    flowie_ingress_t *ingress = flowie_ingress_for(flow, sizeof(packet));
    size_t published = 0u;

    check_not_null(flow);
    check_not_null(ingress);
    check_int_eq(flowie_ingress_feed(ingress, packet, sizeof(packet), &published), TURBO_OK);
    check_size_eq(published, 1u);
    check_size_eq(capture.calls, 1u);
    check_int_eq(capture.buffer_backed[0], 1);
    check_int_eq(capture.owned_payload[0], 0);
    check_int_eq(turbo_flow_pool_snapshot_at(flow, 0u, &pool), TURBO_OK);
    check_size_eq(pool.submitted, 1u);
    check_size_eq(pool.completed, 1u);
    check_mem_eq(capture.packets[0], packet, sizeof(packet));

    flowie_ingress_destroy(ingress);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_ingress_capture_cleanup(&capture);
  }

  it("reports publish completion only after the configured worker graph returns") {
    static const uint8_t packet[] = {0x10, 0x17, 0x00, 0x04, 'M',  'Q',  'T',  'T', 0x05,
                                     0x02, 0x00, 0x3c, 0x07, 0x15, 0x00, 0x04, 'n', 'o',
                                     'n',  'e',  0x00, 0x03, 'c',  'l',  'i'};
    flowie_ingress_capture_t capture = {0};
    flowie_ingress_completion_probe_t probe = {&capture, 0u, 0u, TURBO_EINVAL};
    flowie_ingress_config_t config = FLOWIE_INGRESS_CONFIG_INIT;
    turbo_flow_t *flow = flowie_ingress_flow(" worker 1 capacity 8", &capture);
    flowie_ingress_t *ingress;
    size_t published = 0u;

    check_not_null(flow);
    config.dispatch = flowie_ingress_graph_dispatch;
    config.dispatch_ctx = flow;
    config.max_packet_size = sizeof(packet);
    config.publish_complete = flowie_ingress_completion;
    config.prepare_ctx = &probe;
    ingress = flowie_ingress_create(&config);
    check_not_null(ingress);
    check_int_eq(flowie_ingress_feed(ingress, packet, sizeof(packet), &published), TURBO_OK);
    check_size_eq(published, 1u);
    check_size_eq(probe.calls, 1u);
    check_size_eq(probe.captured_before_callback, 1u);
    check_int_eq(probe.status, TURBO_OK);

    flowie_ingress_destroy(ingress);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_ingress_capture_cleanup(&capture);
  }

  it("reports a worker graph failure without converting it into completion success") {
    static const uint8_t packet[] = {0x10, 0x17, 0x00, 0x04, 'M',  'Q',  'T',  'T', 0x05,
                                     0x02, 0x00, 0x3c, 0x07, 0x15, 0x00, 0x04, 'n', 'o',
                                     'n',  'e',  0x00, 0x03, 'c',  'l',  'i'};
    flowie_ingress_capture_t capture = {0};
    flowie_ingress_completion_probe_t probe = {&capture, 0u, 0u, TURBO_OK};
    flowie_ingress_config_t config = FLOWIE_INGRESS_CONFIG_INIT;
    turbo_flow_t *flow = flowie_ingress_flow(" worker 1 capacity 8", &capture);
    flowie_ingress_t *ingress;
    size_t published = 0u;

    check_not_null(flow);
    capture.result = TURBO_EIO;
    config.dispatch = flowie_ingress_graph_dispatch;
    config.dispatch_ctx = flow;
    config.max_packet_size = sizeof(packet);
    config.publish_complete = flowie_ingress_completion;
    config.prepare_ctx = &probe;
    ingress = flowie_ingress_create(&config);
    check_not_null(ingress);
    check_int_eq(flowie_ingress_feed(ingress, packet, sizeof(packet), &published), TURBO_EIO);
    check_size_eq(published, 0u);
    check_size_eq(probe.calls, 1u);
    check_size_eq(probe.captured_before_callback, 1u);
    check_int_eq(probe.status, TURBO_EIO);

    flowie_ingress_destroy(ingress);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_ingress_capture_cleanup(&capture);
  }

  it("fails fast on packet HWM and graph errors without hidden replay") {
    static const uint8_t oversized[] = {0x30, 0x09};
    static const uint8_t pings[] = {0xc0, 0x00, 0xc0, 0x00};
    flowie_ingress_capture_t capture = {0};
    turbo_flow_t *flow = flowie_ingress_flow("", &capture);
    flowie_ingress_t *ingress = flowie_ingress_for(flow, 8u);
    size_t published = 0u;

    check_not_null(flow);
    check_not_null(ingress);
    check_int_eq(flowie_ingress_feed(ingress, oversized, sizeof(oversized), &published),
                 TURBO_EMSGSIZE);
    check_size_eq(published, 0u);
    check_size_eq(capture.calls, 0u);
    flowie_ingress_destroy(ingress);

    ingress = flowie_ingress_for(flow, 8u);
    check_not_null(ingress);
    capture.result = TURBO_EIO;
    check_int_eq(flowie_ingress_feed(ingress, pings, sizeof(pings), &published), TURBO_EPROTO);
    check_size_eq(published, 0u);
    check_size_eq(capture.calls, 0u);
    capture.result = TURBO_OK;
    check_int_eq(flowie_ingress_feed(ingress, pings, 2u, &published), TURBO_EPROTO);
    check_size_eq(capture.calls, 0u);

    flowie_ingress_destroy(ingress);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    flowie_ingress_capture_cleanup(&capture);
  }
}
