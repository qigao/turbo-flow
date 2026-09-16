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
  int resource_calls;
  uint64_t generation;
  const char *owner;
  int omit_provider;
  int fail_quiesce;
  int fail_resume;
  int reenter;
  int reenter_status;
  const char *uid_override;
  int fail_replace_call;
  int fail_quiesce_call;
  int quiesce_calls;
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
  return SALTS_OK;
}

static int discovery_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  discovery_adapter_t *adapter = ctx;
  *out = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  out->kind = TURBO_FLOW_RESOURCE_CONNECTION;
  out->domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  snprintf(out->uid, sizeof(out->uid), "connection:%s", adapter->owner);
  if (adapter->uid_override)
    snprintf(out->uid, sizeof(out->uid), "%s", adapter->uid_override);
  snprintf(out->owner_name, sizeof(out->owner_name), "%s", adapter->owner);
  out->generation = adapter->generation;
  out->observed_generation = adapter->generation;
  return SALTS_OK;
}

static int discovery_resource_command(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_resource_command_t *command) {
  discovery_adapter_t *adapter = ctx;
  (void)flow;
  adapter->resource_calls++;
  adapter->command_count++;
  if (adapter->reenter) {
    turbo_flow_resource_command_result_t nested = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    adapter->reenter = 0;
    adapter->reenter_status = turbo_flow_resource_command(flow, command, &nested);
  }
  if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_QUIESCE) {
    adapter->quiesce_calls++;
    if (adapter->fail_quiesce || adapter->quiesce_calls == adapter->fail_quiesce_call)
      return SALTS_EIO;
  }
  if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_RESUME && adapter->fail_resume)
    return SALTS_EBUSY;
  switch (command->kind) {
  case TURBO_FLOW_RESOURCE_COMMAND_QUIESCE: adapter->active = 0; break;
  case TURBO_FLOW_RESOURCE_COMMAND_RESUME: adapter->active = 1; break;
  case TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT:
    adapter->replace_count++;
    if (adapter->fail_next_replace || adapter->replace_count == adapter->fail_replace_call) {
      adapter->fail_next_replace = 0;
      return SALTS_EIO;
    }
    snprintf(adapter->host, sizeof(adapter->host), "%s", command->endpoint_host);
    snprintf(adapter->path, sizeof(adapter->path), "%s", command->endpoint_path);
    adapter->port = command->endpoint_port;
    break;
  default: return SALTS_EINVAL;
  }
  adapter->generation++;
  return SALTS_OK;
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
  turbo_flow_resource_provider_ops_t resource_ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
  first->owner = "slot.first";
  second->owner = "slot.second";
  first->generation = second->generation = 1u;
  resource_ops.metadata = discovery_metadata;
  resource_ops.command = discovery_resource_command;
  memset(&ops, 0, sizeof(ops));
  ops.consume = discovery_consume;
  if (!flow || turbo_flow_register_adapter(flow, "slot.first", &ops, first) != SALTS_OK ||
      turbo_flow_register_adapter(flow, "slot.second", &ops, second) != SALTS_OK ||
      (!first->omit_provider && turbo_flow_register_resource_provider(flow, first->owner, &resource_ops, first) != SALTS_OK) ||
      (!second->omit_provider && turbo_flow_register_resource_provider(flow, second->owner, &resource_ops, second) != SALTS_OK) ||
      turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
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

static int discovery_fill_history(turbo_flow_t *flow, discovery_adapter_t *adapter, size_t count) {
  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    int rc;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    snprintf(command.target_uid, sizeof(command.target_uid), "connection:%s", adapter->owner);
    snprintf(command.idempotency_key, sizeof(command.idempotency_key), "fill:%llu",
             (unsigned long long)adapter->generation);
    command.expected_generation = adapter->generation;
    rc = turbo_flow_resource_command(flow, &command, &result);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

spec("flow_discovery") {
  it("rejects ambiguous providers before any create side effect") {
    const char *names[] = {"slot.first", "slot.second"};
    discovery_adapter_t first = {0}, second = {0}, extra = {0};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    check_not_null(flow);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    extra.owner = "slot.second"; extra.generation = 1u; extra.uid_override = "connection:extra";
    ops.metadata = discovery_metadata; ops.command = discovery_resource_command;
    check_equal(turbo_flow_register_resource_provider(flow, extra.owner, &ops, &extra), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_EPROTO);
    check_null(controller);
    check_equal(first.command_count + second.command_count + extra.command_count, 0);
    turbo_flow_destroy(flow);
  }

  it("keeps the captured UID and reads current generation across independent controllers") {
    const char *names[] = {"slot.first"};
    discovery_adapter_t first = {0}, second = {0};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *one = NULL, *two = NULL;
    turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    turbo_flow_discovery_peer_t peer = discovery_peer("peer", "localhost", 7101);
    turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    check_not_null(flow);
    config.flow = flow; config.adapter_names = names; config.adapter_count = 1u;
    check_equal(turbo_flow_discovery_controller_create(&config, &one), SALTS_OK);
    check_equal(turbo_flow_discovery_controller_create(&config, &two), SALTS_OK);
    check_equal(first.generation, 3u);
    check_equal(discovery_fill_history(flow, &first, 1u), SALTS_OK);
    list.registry_version = 1u; list.peers = &peer; list.peer_count = 1u;
    check_equal(turbo_flow_discovery_replace_peer_list(one, &list, &result), SALTS_OK);
    check_equal(first.generation, 6u);
    first.uid_override = "connection:replacement";
    peer.port = 7201; list.registry_version = 2u;
    check_equal(turbo_flow_discovery_replace_peer_list(one, &list, &result), SALTS_ENOENT);
    check_equal(first.port, 7101);
    check_equal(turbo_flow_discovery_registry_version(one), 1u);
    turbo_flow_discovery_controller_destroy(two);
    turbo_flow_discovery_controller_destroy(one);
    turbo_flow_destroy(flow);
  }

  it("reserves the entire create compensation budget before any side effects") {
    const char *names[] = {"slot.first", "slot.second"};
    for (size_t available = 3u; available <= 4u; ++available) {
      discovery_adapter_t first = {0}, second = {0};
      turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
      turbo_flow_discovery_controller_t *controller = NULL;
      turbo_flow_t *flow = discovery_started_flow(&first, &second);
      int before;
      check_not_null(flow);
      check_equal(discovery_fill_history(flow, &first,
                    TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX - available), SALTS_OK);
      before = first.command_count;
      second.fail_quiesce = 1;
      config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
      check_equal(turbo_flow_discovery_controller_create(&config, &controller),
                  available == 3u ? SALTS_ENOSPC : SALTS_EIO);
      check_null(controller);
      check_true(first.active);
      check_equal(first.command_count - before, available == 3u ? 0 : 2);
      check_equal(second.command_count, available == 3u ? 0 : 1);
      check_equal(discovery_fill_history(flow, &first, 1u), SALTS_OK);
      turbo_flow_destroy(flow);
    }
  }

  it("retains peer facts when an exact mixed replacement budget compensates failure") {
    const char *names[] = {"slot.first", "slot.second"};
    for (size_t available = 4u; available <= 5u; ++available) {
      discovery_adapter_t first = {0}, second = {0};
      turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
      turbo_flow_discovery_controller_t *controller = NULL;
      turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
      turbo_flow_discovery_peer_t peers[2];
      turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
      turbo_flow_t *flow = discovery_started_flow(&first, &second);
      int before;
      check_not_null(flow);
      config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
      check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_OK);
      peers[0] = discovery_peer("first", "localhost", 7101);
      list.registry_version = 1u; list.peers = peers; list.peer_count = 1u;
      check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
      /* Two create records and two add records already committed. A=1,U=1 => 5. */
      check_equal(discovery_fill_history(flow, &first,
                    TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX - available - 4u), SALTS_OK);
      before = first.command_count + second.command_count;
      peers[0].port = 7201;
      peers[1] = discovery_peer("second", "localhost", 7202);
      list.registry_version = 2u; list.peer_count = 2u;
      second.fail_resume = 1;
      check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result),
                  available == 4u ? SALTS_ENOSPC : SALTS_EBUSY);
      check_equal(result.rollback_status, SALTS_OK);
      check_equal(first.command_count + second.command_count - before, available == 4u ? 0 : 5);
      check_equal(first.port, 7101);
      check_true(first.active);
      check_false(second.active);
      check_equal(turbo_flow_discovery_registry_version(controller), 1u);
      check_equal(turbo_flow_discovery_active_peer_count(controller), 1u);
      /* Replays and newer unchanged sets must still succeed with a full history. */
      peers[0].port = 7101; list.peer_count = 1u; list.registry_version = 1u;
      before = first.command_count + second.command_count;
      check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
      list.registry_version = 3u;
      check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
      check_equal(first.command_count + second.command_count, before);
      turbo_flow_discovery_controller_destroy(controller);
      turbo_flow_destroy(flow);
    }
  }

  it("exposes failed replacement compensation and refuses later operations") {
    const char *names[] = {"slot.first", "slot.second"};
    discovery_adapter_t first = {0}, second = {0};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    turbo_flow_discovery_peer_t peers[2];
    turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
    discovery_source_probe_t probe = {SALTS_EIO, 0};
    turbo_flow_discovery_source_t source = TURBO_FLOW_DISCOVERY_SOURCE_INIT;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    check_not_null(flow);
    config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_OK);
    peers[0] = discovery_peer("first", "localhost", 7101);
    list.registry_version = 1u; list.peers = peers; list.peer_count = 1u;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
    peers[0].port = 7201; peers[1] = discovery_peer("second", "localhost", 7202);
    list.registry_version = 2u; list.peer_count = 2u;
    first.fail_replace_call = 3;
    second.fail_resume = 1;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_EIO);
    check_equal(result.status, SALTS_EBUSY);
    check_equal(result.rollback_status, SALTS_EIO);
    check_equal(first.port, 7201);
    check_false(second.active);
    check_equal(turbo_flow_discovery_registry_version(controller), 1u);
    check_equal(turbo_flow_discovery_active_peer_count(controller), 1u);
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_EINVAL);
    source.fetch = discovery_fetch_error; source.ctx = &probe;
    check_equal(turbo_flow_discovery_poll(controller, &source, &result), SALTS_EINVAL);
    check_equal(probe.calls, 0);
    check_equal(discovery_fill_history(flow, &first, 1u), SALTS_OK);
    turbo_flow_discovery_controller_destroy(controller);
    turbo_flow_destroy(flow);
  }

  it("budgets removal and resumes earlier removed slots after a later owner failure") {
    const char *names[] = {"slot.first", "slot.second"};
    for (size_t available = 3u; available <= 4u; ++available) {
      discovery_adapter_t first = {0}, second = {0};
      turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
      turbo_flow_discovery_controller_t *controller = NULL;
      turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
      turbo_flow_discovery_peer_t peers[2];
      turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
      turbo_flow_t *flow = discovery_started_flow(&first, &second);
      int before;
      check_not_null(flow);
      config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
      check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_OK);
      peers[0] = discovery_peer("first", "localhost", 7101);
      peers[1] = discovery_peer("second", "localhost", 7102);
      list.registry_version = 1u; list.peers = peers; list.peer_count = 2u;
      check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
      check_equal(discovery_fill_history(flow, &first,
                    TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX - available - 6u), SALTS_OK);
      before = first.command_count + second.command_count;
      second.fail_quiesce = 1;
      list.registry_version = 2u; list.peer_count = 0u;
      check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result),
                  available == 3u ? SALTS_ENOSPC : SALTS_EIO);
      check_equal(result.rollback_status, SALTS_OK);
      check_equal(first.command_count + second.command_count - before, available == 3u ? 0 : 3);
      check_true(first.active);
      check_true(second.active);
      check_equal(turbo_flow_discovery_registry_version(controller), 1u);
      check_equal(turbo_flow_discovery_active_peer_count(controller), 2u);
      turbo_flow_discovery_controller_destroy(controller);
      turbo_flow_destroy(flow);
    }
  }

  it("preflights all stable providers before quiescing any slot") {
    const char *names[] = {"slot.first", "slot.second"};
    discovery_adapter_t first = {0}, second = {0};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    second.omit_provider = 1;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    check_not_null(flow);
    config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_ENOENT);
    check_null(controller);
    check_equal(first.command_count, 0);
    check_equal(second.command_count, 0);
    turbo_flow_destroy(flow);
  }

  it("resumes earlier create slots and reports failed compensation") {
    const char *names[] = {"slot.first", "slot.second"};
    discovery_adapter_t first = {0}, second = {0};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    check_not_null(flow);
    config.flow = flow; config.adapter_names = names; config.adapter_count = 2u;
    second.fail_quiesce = 1;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_EIO);
    check_null(controller);
    check_true(first.active);
    check_equal(first.generation, 3u);
    first.fail_resume = 1;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_EBUSY);
    check_null(controller);
    check_false(first.active);
    {
      char original_code[32], rollback_code[32];
      snprintf(original_code, sizeof(original_code), "(%d)", SALTS_EIO);
      snprintf(rollback_code, sizeof(rollback_code), "(%d)", SALTS_EBUSY);
      check_equal(turbo_flow_last_error(flow)->code, SALTS_EBUSY);
      check_contains(turbo_flow_last_error(flow)->message, original_code);
      check_contains(turbo_flow_last_error(flow)->message, rollback_code);
    }
    turbo_flow_destroy(flow);
  }

  it("blocks callback command reentry and stops polling before fetching") {
    const char *names[] = {"slot.first"};
    discovery_adapter_t first = {0}, second = {0};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
    discovery_source_probe_t probe = {SALTS_EIO, 0};
    turbo_flow_discovery_source_t source = TURBO_FLOW_DISCOVERY_SOURCE_INIT;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    check_not_null(flow);
    first.reenter = 1;
    config.flow = flow; config.adapter_names = names; config.adapter_count = 1u;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_OK);
    check_equal(first.reenter_status, SALTS_EBUSY);
    check_equal(first.resource_calls, 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    list.registry_version = 1u;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_EINVAL);
    source.fetch = discovery_fetch_error; source.ctx = &probe;
    check_equal(turbo_flow_discovery_poll(controller, &source, &result), SALTS_EINVAL);
    check_equal(probe.calls, 0);
    turbo_flow_discovery_controller_destroy(controller);
    turbo_flow_destroy(flow);
  }

  it("reconciles versioned peer sets and rolls owner commands back atomically") {
    static const char *adapter_names[] = {"slot.first", "slot.second"};
    discovery_adapter_t first = {1, 0, 0, 0, "initial-a", "", 7001};
    discovery_adapter_t second = {1, 0, 0, 0, "initial-b", "", 7002};
    turbo_flow_discovery_controller_config_t config = TURBO_FLOW_DISCOVERY_CONTROLLER_CONFIG_INIT;
    turbo_flow_discovery_controller_t *controller = NULL;
    turbo_flow_discovery_replace_result_t result = TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    turbo_flow_discovery_peer_t peers[2];
    turbo_flow_discovery_peer_list_t list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
    discovery_source_probe_t source_probe = {SALTS_EIO, 0};
    turbo_flow_discovery_source_t source = TURBO_FLOW_DISCOVERY_SOURCE_INIT;
    turbo_flow_t *flow = discovery_started_flow(&first, &second);
    int commands_before;

    check_not_null(flow);
    config.flow = flow;
    config.adapter_names = adapter_names;
    config.adapter_count = 2u;
    check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_OK);
    check_not_null(controller);
    check_equal(first.resource_calls, 1);
    check_equal(first.generation, 2u);
    check_false(first.active);
    check_false(second.active);

    peers[0] = discovery_peer("peer-a", "127.0.0.1", 7101);
    list.registry_version = 1u;
    list.peers = peers;
    list.peer_count = 1u;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
    check_equal(result.added, 1u);
    check_equal(turbo_flow_discovery_registry_version(controller), 1u);
    check_equal(turbo_flow_discovery_active_peer_count(controller), 1u);
    check_true(first.active);
    check_equal(first.host, "127.0.0.1");
    check_equal(first.port, 7101);

    commands_before = first.command_count + second.command_count;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
    check_equal(result.unchanged, 1u);
    check_equal(first.command_count + second.command_count, commands_before);

    peers[0] = discovery_peer("peer-a", "127.0.0.1", 7199);
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_EPROTO);
    check_equal(first.port, 7101);

    peers[0] = discovery_peer("peer-a", "127.0.0.1", 7201);
    peers[1] = discovery_peer("peer-b", "127.0.0.1", 7202);
    list.registry_version = 2u;
    list.peer_count = 2u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
    check_equal(result.updated, 1u);
    check_equal(result.added, 1u);
    check_true(second.active);

    list.registry_version = 1u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result),
                 SALTS_EALREADY);
    check_equal(turbo_flow_discovery_registry_version(controller), 2u);

    peers[0] = discovery_peer("peer-b", "127.0.0.1", 7202);
    list.registry_version = 3u;
    list.peer_count = 1u;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_OK);
    check_equal(result.removed, 1u);
    check_false(first.active);
    check_true(second.active);

    source.fetch = discovery_fetch_error;
    source.ctx = &source_probe;
    commands_before = first.command_count + second.command_count;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_poll(controller, &source, &result), SALTS_EIO);
    check_equal(source_probe.calls, 1);
    check_equal(first.command_count + second.command_count, commands_before);
    check_equal(turbo_flow_discovery_registry_version(controller), 3u);

    peers[0] = discovery_peer("peer-c", "127.0.0.1", 7401);
    peers[1] = discovery_peer("peer-b", "127.0.0.1", 7402);
    list.registry_version = 4u;
    list.peer_count = 2u;
    second.fail_next_replace = 1;
    result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
    check_equal(turbo_flow_discovery_replace_peer_list(controller, &list, &result), SALTS_EIO);
    check_equal(result.status, SALTS_EIO);
    check_equal(result.rollback_status, SALTS_OK);
    check_equal(turbo_flow_discovery_registry_version(controller), 3u);
    check_equal(turbo_flow_discovery_active_peer_count(controller), 1u);
    check_false(first.active);
    check_true(second.active);
    check_equal(second.port, 7202);

    turbo_flow_discovery_controller_destroy(controller);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
