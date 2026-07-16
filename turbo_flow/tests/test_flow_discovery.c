#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_discovery.h"

#include <stdio.h>
#include <string.h>

typedef struct discovery_adapter_s {
  int active;
  int command_count;
  int replace_count;
  int fail_next_replace;
  char host[TURBO_FLOW_ENDPOINT_MAX + 1u];
  char path[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int port;
} discovery_adapter_t;

typedef struct discovery_source_probe_s {
  int status;
  int calls;
} discovery_source_probe_t;

static int discovery_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                             turbo_flow_msg_t *msg) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)msg;
  return TURBO_OK;
}

static int discovery_adapter_command(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_adapter_command_t *command) {
  discovery_adapter_t *adapter = (discovery_adapter_t *)ctx;
  (void)flow;
  adapter->command_count += 1;
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    adapter->active = 0;
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    adapter->active = 1;
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    adapter->replace_count += 1;
    if (adapter->fail_next_replace) {
      adapter->fail_next_replace = 0;
      return TURBO_EIO;
    }
    if (snprintf(adapter->host, sizeof(adapter->host), "%s", command->endpoint.host) < 0 ||
        snprintf(adapter->path, sizeof(adapter->path), "%s", command->endpoint.path) < 0) {
      return TURBO_EIO;
    }
    adapter->port = command->endpoint.port;
    return TURBO_OK;
  default:
    return TURBO_EINVAL;
  }
}

static turbo_flow_t *discovery_started_flow(discovery_adapter_t *first,
                                            discovery_adapter_t *second) {
  static const char dsl[] = "source input\n"
                            "stage first adapter slot.first\n"
                            "stage second adapter slot.second\n"
                            "stage main {\n"
                            "  input -> first\n"
                            "  input -> second\n"
                            "}\n";
  turbo_flow_adapter_ops_t ops;
  turbo_flow_t *flow = turbo_flow_create();
  memset(&ops, 0, sizeof(ops));
  ops.consume = discovery_consume;
  ops.command = discovery_adapter_command;
  if (!flow || turbo_flow_register_adapter(flow, "slot.first", &ops, first) != TURBO_OK ||
      turbo_flow_register_adapter(flow, "slot.second", &ops, second) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK || turbo_flow_start(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_discovery_peer_t discovery_peer(const char *id, const char *host, int port) {
  turbo_flow_discovery_peer_t peer = TURBO_FLOW_DISCOVERY_PEER_INIT;
  (void)snprintf(peer.peer_id, sizeof(peer.peer_id), "%s", id);
  (void)snprintf(peer.host, sizeof(peer.host), "%s", host);
  peer.port = port;
  return peer;
}

static int discovery_fetch_error(void *ctx, uint64_t *registry_version,
                                 turbo_flow_discovery_peer_t *peers, size_t capacity,
                                 size_t *count) {
  discovery_source_probe_t *probe = (discovery_source_probe_t *)ctx;
  (void)registry_version;
  (void)peers;
  (void)capacity;
  (void)count;
  probe->calls += 1;
  return probe->status;
}

spec("flow_discovery") {
  it("reconciles versioned peer sets and rolls owner commands back atomically") {
    static const char *adapter_names[] = {"slot.first", "slot.second"};
    discovery_adapter_t first = {1, 0, 0, 0, "initial-a", "", 7001};
    discovery_adapter_t second = {1, 0, 0, 0, "initial-b", "", 7002};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    turbo_flow_discovery_peer_t peers[2];
    turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
    discovery_source_probe_t source_probe = {TURBO_EIO, 0};
    turbo_flow_discovery_source_t source = TURBO_FLOW_DISCOVERY_SOURCE_INIT;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    int commands_before;

    check_not_null(flow);
    config.flow = flow;
    config.adapter_names = adapter_names;
    config.adapter_count = 2u;
    check_int_eq(turbo_flow_discovery_controller_create(&config, &controller), TURBO_OK);
    check_not_null(controller);
    check_false(first.active);
    check_false(second.active);

    peers[0] = discovery_peer("peer-a", "127.0.0.1", 7101);
    list.registry_version = 1u;
    list.peers = peers;
    list.peer_count = 1u;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_OK);
    check_uint_eq(result.added, 1u);
    check_uint_eq(turbo_flow_discovery_registry_version(controller), 1u);
    check_uint_eq(turbo_flow_discovery_active_peer_count(controller), 1u);
    check_true(first.active);
    check_str_eq(first.host, "127.0.0.1");
    check_int_eq(first.port, 7101);

    commands_before = first.command_count + second.command_count;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_OK);
    check_uint_eq(result.unchanged, 1u);
    check_int_eq(first.command_count + second.command_count, commands_before);

    peers[0] = discovery_peer("peer-a", "127.0.0.1", 7199);
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_EPROTO);
    check_int_eq(first.port, 7101);

    peers[0] = discovery_peer("peer-a", "127.0.0.1", 7201);
    peers[1] = discovery_peer("peer-b", "127.0.0.1", 7202);
    list.registry_version = 2u;
    list.peer_count = 2u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_OK);
    check_uint_eq(result.updated, 1u);
    check_uint_eq(result.added, 1u);
    check_true(second.active);

    list.registry_version = 1u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result),
                 TURBO_EALREADY);
    check_uint_eq(turbo_flow_discovery_registry_version(controller), 2u);

    peers[0] = discovery_peer("peer-b", "127.0.0.1", 7202);
    list.registry_version = 3u;
    list.peer_count = 1u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_OK);
    check_uint_eq(result.removed, 1u);
    check_false(first.active);
    check_true(second.active);

    source.fetch = discovery_fetch_error;
    source.ctx = &source_probe;
    commands_before = first.command_count + second.command_count;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_poll(controller, &source, &result), TURBO_EIO);
    check_int_eq(source_probe.calls, 1);
    check_int_eq(first.command_count + second.command_count, commands_before);
    check_uint_eq(turbo_flow_discovery_registry_version(controller), 3u);

    peers[0] = discovery_peer("peer-c", "127.0.0.1", 7401);
    peers[1] = discovery_peer("peer-b", "127.0.0.1", 7402);
    list.registry_version = 4u;
    list.peer_count = 2u;
    second.fail_next_replace = 1;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_int_eq(turbo_flow_discovery_replace_peer_list(controller, &list, &result), TURBO_EIO);
    check_int_eq(result.status, TURBO_EIO);
    check_int_eq(result.rollback_status, TURBO_OK);
    check_uint_eq(turbo_flow_discovery_registry_version(controller), 3u);
    check_uint_eq(turbo_flow_discovery_active_peer_count(controller), 1u);
    check_false(first.active);
    check_true(second.active);
    check_int_eq(second.port, 7202);

    turbo_flow_discovery_controller_destroy(controller);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }
}
