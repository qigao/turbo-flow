#include "flow_databind_internal.h"
#include "tinytest.h"
#include "turbo_flow_databind.h"
#include "turbo_flow_domain.h"

#include "tf_channel.flowmq.h"
#include "tf_channel.socket.h"

#include <string.h>

typedef struct databind_channel_sink_probe_s {
  size_t calls;
} databind_channel_sink_probe_t;

static size_t socket_resolver_calls;
static DataBindSocketNativeBindingResolverFn socket_generated_resolver;
static DataBindFlowMQNativeBindingResolverFn flowmq_generated_resolver;

static DataBindStatus counting_socket_resolver(
    DataBindNativeTypeBinding *out, DataBindError *error) {
  ++socket_resolver_calls;
  return socket_generated_resolver
             ? socket_generated_resolver(out, error)
             : DATA_BIND_ERR_RUNTIME;
}

static const DataBindNativeStateBinding mismatched_presence[] = {
    {sizeof(DataBindNativeStateBinding), "sequence", 0u, 0u}};

static DataBindStatus mismatched_flowmq_resolver(
    DataBindNativeTypeBinding *out, DataBindError *error) {
  DataBindStatus status =
      flowmq_generated_resolver
          ? flowmq_generated_resolver(out, error)
          : DATA_BIND_ERR_RUNTIME;
  if (status != DATA_BIND_OK || !out) return status;
  out->presence = mismatched_presence;
  out->presence_count =
      sizeof(mismatched_presence) / sizeof(mismatched_presence[0]);
  return DATA_BIND_OK;
}

static int databind_channel_sink(turbo_flow_msg_t *message, void *ctx) {
  databind_channel_sink_probe_t *probe =
      (databind_channel_sink_probe_t *)ctx;
  (void)message;
  if (!probe) return SALTS_EINVAL;
  ++probe->calls;
  return SALTS_OK;
}

static int register_native_sink(
    turbo_flow_t *flow, databind_channel_sink_probe_t *probe) {
  turbo_flow_operation_descriptor_t operation;
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;

  if (!flow || !probe) return SALTS_EINVAL;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = "test.databind.channel.sink";
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = "TelemetryEvent";
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = "TelemetryEvent";
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
  operation.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  operation.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  operation.runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;

  provider.operation_name = operation.name;
  provider.fn = databind_channel_sink;
  provider.ctx = probe;

  {
    int rc = turbo_flow_register_operation(flow, &operation);
    if (rc != SALTS_OK) return rc;
  }
  return turbo_flow_register_operation_provider(flow, &provider);
}

static const char DATABIND_CHANNEL_GRAPH[] =
    "source socket_input\n"
    "source flowmq_input\n"
    "stage sink operation test.databind.channel.sink\n"
    "stage main {\n"
    "  socket_input -> sink\n"
    "  flowmq_input -> sink\n"
    "}\n";

static turbo_flow_t *databind_channel_flow(
    databind_channel_sink_probe_t *probe) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  if (register_native_sink(flow, probe) != SALTS_OK ||
      turbo_flow_parse_string(
          flow, DATABIND_CHANNEL_GRAPH,
          sizeof(DATABIND_CHANNEL_GRAPH) - 1u) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

spec("TurboFlow generated DataBind Channel plan") {
  it("deduplicates Socket and FlowMQ Sources into one canonical Channel") {
    databind_channel_sink_probe_t probe = {0};
    turbo_flow_t *flow = databind_channel_flow(&probe);
    DataBindSocketPlan socket_plan = databind_tf_channel_socket_plan;
    const DataBindFlowMQChannelPlan *flowmq_plan =
        &databind_tf_channel_flowmq_channel_plan;
    const flow_databind_channel_plan_t *channel;
    const flow_runtime_node_plan_t *socket_node;
    const flow_runtime_node_plan_t *flowmq_node;
    turbo_flow_msg_t message;
    int socket_stage;
    int flowmq_stage;

    check_not_null(flow);
    check_equal(strcmp(socket_plan.channel_name, "Device.Telemetry"), 0);
    check_equal(strcmp(flowmq_plan->channel_name, "Device.Telemetry"), 0);
    check_equal(strcmp(socket_plan.message_type, "TelemetryEvent"), 0);
    check_equal(strcmp(flowmq_plan->message_type, "TelemetryEvent"), 0);

    socket_resolver_calls = 0u;
    socket_generated_resolver = socket_plan.native_binding;
    socket_plan.native_binding = counting_socket_resolver;

    check_equal(
        turbo_flow_product_bind_databind_socket_source(
            flow, "socket_input", &socket_plan),
        SALTS_OK);
    check_equal(socket_resolver_calls, (size_t)1u);
    check_equal(
        turbo_flow_product_bind_databind_flowmq_source(
            flow, "flowmq_input", flowmq_plan),
        SALTS_OK);

    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(socket_resolver_calls, (size_t)1u);
    check_equal(vec_size(&flow->compiled_plan.databind_channels), (size_t)1u);

    socket_stage = turbo_flow_find_stage(flow, "socket_input");
    flowmq_stage = turbo_flow_find_stage(flow, "flowmq_input");
    check(socket_stage >= 0);
    check(flowmq_stage >= 0);
    socket_node = socket_stage >= 0
                      ? (const flow_runtime_node_plan_t *)vec_at_const(
                            &flow->compiled_plan.nodes,
                            (size_t)socket_stage)
                      : NULL;
    flowmq_node = flowmq_stage >= 0
                      ? (const flow_runtime_node_plan_t *)vec_at_const(
                            &flow->compiled_plan.nodes,
                            (size_t)flowmq_stage)
                      : NULL;
    check_not_null(socket_node);
    check_not_null(flowmq_node);
    check_equal(socket_node->databind_channel_index, (uint32_t)0u);
    check_equal(flowmq_node->databind_channel_index, (uint32_t)0u);
    check_equal(socket_node->databind_transport,
                FLOW_DATABIND_TRANSPORT_SOCKET);
    check_equal(flowmq_node->databind_transport,
                FLOW_DATABIND_TRANSPORT_FLOWMQ);
    check_equal(socket_node->databind_format, DATA_BIND_FORMAT_BINARY);
    check_equal(flowmq_node->databind_format, DATA_BIND_FORMAT_BINARY);
    check_true(socket_node->databind_transport_plan == &socket_plan);
    check_true(flowmq_node->databind_transport_plan == flowmq_plan);

    channel = (const flow_databind_channel_plan_t *)vec_at_const(
        &flow->compiled_plan.databind_channels, 0u);
    check_not_null(channel);
    check_equal(strcmp(channel->channel_name, "Device.Telemetry"), 0);
    check_equal(strcmp(channel->message_type, "TelemetryEvent"), 0);
    check_not_null(channel->data_stable_id);
    check_not_null(channel->native.data);
    check_equal(strcmp(channel->native.idl_type_name, "TelemetryEvent"), 0);
    check_true(cmeta_data_desc_valid(channel->native.data));

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(
        turbo_flow_publish(flow, "socket_input", &message), SALTS_OK);
    check_equal(
        turbo_flow_publish(flow, "flowmq_input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)2u);
    check_equal(socket_resolver_calls, (size_t)1u);
    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    /*
     * Stage-level generated bindings belong to the parsed generation. A reset
     * destroys them; the next topology may compile, but it has no DataBind
     * Channel contract until Product binds the new Sources again.
     */
    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
    check_equal(
        turbo_flow_parse_string(
            flow, DATABIND_CHANNEL_GRAPH,
            sizeof(DATABIND_CHANNEL_GRAPH) - 1u),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.databind_channels), (size_t)0u);
    socket_stage = turbo_flow_find_stage(flow, "socket_input");
    check(socket_stage >= 0);
    socket_node = socket_stage >= 0
                      ? (const flow_runtime_node_plan_t *)vec_at_const(
                            &flow->compiled_plan.nodes,
                            (size_t)socket_stage)
                      : NULL;
    check_not_null(socket_node);
    check_equal(socket_node->databind_channel_index, FLOW_PLAN_INDEX_NONE);
    check_equal(socket_resolver_calls, (size_t)1u);

    turbo_flow_destroy(flow);
  }

  it("rejects divergent generated message and native overlay contracts") {
    databind_channel_sink_probe_t probe = {0};
    turbo_flow_t *flow = databind_channel_flow(&probe);
    DataBindFlowMQChannelPlan bad_message =
        databind_tf_channel_flowmq_channel_plan;
    DataBindFlowMQChannelPlan bad_overlay =
        databind_tf_channel_flowmq_channel_plan;

    check_not_null(flow);
    check_equal(
        turbo_flow_product_bind_databind_socket_source(
            flow, "socket_input", &databind_tf_channel_socket_plan),
        SALTS_OK);

    bad_message.message_type = "OtherEvent";
    check_equal(
        turbo_flow_product_bind_databind_flowmq_source(
            flow, "flowmq_input", &bad_message),
        SALTS_EPROTO);
    check_false(flow->compiled_plan.sealed);

    flowmq_generated_resolver =
        databind_tf_channel_flowmq_channel_plan.native_binding;
    bad_overlay.native_binding = mismatched_flowmq_resolver;
    check_equal(
        turbo_flow_product_bind_databind_flowmq_source(
            flow, "flowmq_input", &bad_overlay),
        SALTS_EPROTO);
    check_false(flow->compiled_plan.sealed);

    turbo_flow_destroy(flow);
  }

  it("rejects generated Channel bindings on non-Source stages") {
    databind_channel_sink_probe_t probe = {0};
    turbo_flow_t *flow = databind_channel_flow(&probe);

    check_not_null(flow);
    check_equal(
        turbo_flow_product_bind_databind_socket_source(
            flow, "sink", &databind_tf_channel_socket_plan),
        SALTS_ENOTSUP);
    turbo_flow_destroy(flow);
  }
}
