#include "chttp_plugin_fixtures.h"
#include "turbo_flow_plugin_generation.h"
#include <stdio.h>
#include <string.h>

enum { CONSUMER_TIMEOUT_MS = 1000, CONSUMER_PROVIDER_COUNT = 3 };

static int discard_response(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

static int run_kind(turbo_flow_plugin_host_t *host, size_t kind,
                    turbo_flow_plugin_generation_t **cleanup_owner) {
  static const char *const yamls[] = {client_yaml, server_yaml, websocket_yaml};
  static const char *const graphs[] = {
      "source input\nstage request adapter client\nstage "
      "output operation consumer.discard\nstage main {\n input -> request -> output\n}\n",
      "source input adapter server\nstage output adapter "
      "server\nstage main {\n input -> output\n}\n",
      "source input adapter websocket\nstage output adapter "
      "websocket\nstage main {\n input -> output\n}\n"};
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_plugin_generation_t *generation = NULL;
  turbo_flow_t *flow = NULL;
  int rc = turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe);
  if (rc != SALTS_OK) goto cleanup;
  rc = turbo_flow_config_resolve_yaml(yamls[kind], strlen(yamls[kind]), &resolved, &ce);
  if (rc != SALTS_OK) goto cleanup;
  flow = turbo_flow_create();
  if (!flow) {
    rc = SALTS_ENOMEM;
    goto cleanup;
  }
  rc = turbo_flow_parse_string(flow, graphs[kind], strlen(graphs[kind]));
  if (rc != SALTS_OK) goto cleanup;
  if (kind == 0u) {
    turbo_flow_operation_descriptor_t operation = {0};
    turbo_flow_operation_provider_registration_t provider =
        TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
    operation.size = sizeof(operation);
    operation.name = "consumer.discard";
    operation.version = 1u;
    operation.domain = TURBO_FLOW_DOMAIN_DATA;
    operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.input_type = "Message";
    operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.output_type = "Message";
    operation.flags = TURBO_FLOW_OPERATION_STAGE;
    operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operation.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
    operation.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
    operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
    operation.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
    operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
    provider.operation_name = operation.name;
    provider.fn = discard_response;
    rc = turbo_flow_register_operation(flow, &operation);
    if (rc != SALTS_OK) goto cleanup;
    rc = turbo_flow_register_operation_provider(flow, &provider);
    if (rc != SALTS_OK) goto cleanup;
  }
  rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, NULL, &generation,
                                           cleanup_owner, &ce);
  if (rc != SALTS_OK) goto cleanup;
  turbo_flow_resolved_config_destroy(resolved);
  resolved = NULL;
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  snapshot = NULL;
  if (flow || turbo_flow_plugin_generation_owner_count(generation) != 1u) {
    rc = SALTS_EPROTO;
    goto cleanup;
  }
  rc = turbo_flow_start(turbo_flow_plugin_generation_flow(generation));
  if (rc != SALTS_OK) goto cleanup;
  rc = turbo_flow_plugin_generation_poll(generation, 0u, &ce);
  if (rc != SALTS_OK) goto cleanup;
  if (kind == 1u) {
    turbo_flow_t *generation_flow = turbo_flow_plugin_generation_flow(generation);
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    const size_t managed_count = turbo_flow_managed_boundary_count(generation_flow);
    if (managed_count != 1u) {
      fprintf(stderr, "CHTTP server managed boundary count: expected 1, actual %zu\n",
              managed_count);
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    rc = turbo_flow_managed_boundary_descriptor_at(generation_flow, 0u, &descriptor);
    if (rc != SALTS_OK ||
        descriptor.role_flags !=
            (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_SOURCE | TURBO_FLOW_MANAGED_BOUNDARY_SINK) ||
        descriptor.command_flags !=
            (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE |
                       TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME)) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    rc = turbo_flow_managed_boundary_snapshot_at(generation_flow, 0u, &managed);
    if (rc != SALTS_OK || managed.state != TURBO_FLOW_MANAGED_BOUNDARY_RUNNING ||
        managed.in_flight != 0u) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = managed.generation;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "consumer-http-quiesce",
           sizeof("consumer-http-quiesce"));
    rc = turbo_flow_resource_command(generation_flow, &command, &result);
    if (rc != SALTS_OK) goto cleanup;
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    rc = turbo_flow_managed_boundary_snapshot_at(generation_flow, 0u, &managed);
    if (rc != SALTS_OK || managed.state != TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    command.expected_generation = managed.generation;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "consumer-http-resume", sizeof("consumer-http-resume"));
    rc = turbo_flow_resource_command(generation_flow, &command, &result);
    if (rc != SALTS_OK) goto cleanup;
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    rc = turbo_flow_managed_boundary_snapshot_at(generation_flow, 0u, &managed);
    if (rc != SALTS_OK || managed.state != TURBO_FLOW_MANAGED_BOUNDARY_RUNNING) {
      rc = SALTS_EPROTO;
      goto cleanup;
    }
  }
  rc = turbo_flow_plugin_generation_lease_acquire(generation);
  if (rc != SALTS_OK) goto cleanup;
  int lease_busy = turbo_flow_plugin_generation_destroy(generation, CONSUMER_TIMEOUT_MS, &ce);
  int host_busy = turbo_flow_plugin_host_destroy(host, CONSUMER_TIMEOUT_MS, &pe);
  rc = turbo_flow_plugin_generation_lease_release(generation);
  if (rc != SALTS_OK) goto cleanup;
  if (lease_busy != SALTS_EBUSY || host_busy != SALTS_EBUSY) {
    rc = SALTS_EPROTO;
    goto cleanup;
  }
cleanup:
  if (*cleanup_owner) {
    int cleanup_rc = turbo_flow_plugin_generation_destroy(*cleanup_owner, CONSUMER_TIMEOUT_MS, &ce);
    if (cleanup_rc == SALTS_OK) *cleanup_owner = NULL;
    else rc = cleanup_rc;
  }
  if (generation) {
    int destroy_rc = turbo_flow_plugin_generation_destroy(generation, CONSUMER_TIMEOUT_MS, &ce);
    if (destroy_rc != SALTS_OK) {
      rc = destroy_rc;
      *cleanup_owner = generation;
    }
  }
  turbo_flow_destroy(flow);
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  if (rc != SALTS_OK)
    fprintf(stderr, "CHTTP kind %zu failed (%d): %s %s\n", kind, rc, ce.path, ce.message);
  return rc;
}

int main(int argc, char **argv) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_host_t *host = NULL;
  turbo_flow_plugin_generation_t *cleanup_owner = NULL;
  if (argc != 2 || !argv[1][0]) return 1;
  config.module_capacity = 1u;
  config.transactional_adapter_provider_capacity = CONSUMER_PROVIDER_COUNT;
  int rc = turbo_flow_plugin_host_create(&config, &host, &error);
  if (rc != SALTS_OK) return 1;
  rc = turbo_flow_plugin_host_load(host, argv[1], &error);
  if (rc != SALTS_OK)
    fprintf(stderr, "CHTTP consumer failed at plugin DLL load: %s\n", error.message);
  if (rc == SALTS_OK &&
      turbo_flow_plugin_host_transactional_adapter_provider_count(host) != CONSUMER_PROVIDER_COUNT)
    rc = SALTS_EPROTO;
  for (size_t i = 0u; rc == SALTS_OK && i < CONSUMER_PROVIDER_COUNT; ++i)
    rc = run_kind(host, i, &cleanup_owner);
  if (cleanup_owner) {
    turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    int cleanup_rc =
        turbo_flow_plugin_generation_destroy(cleanup_owner, CONSUMER_TIMEOUT_MS, &cleanup_error);
    if (cleanup_rc == SALTS_OK) cleanup_owner = NULL;
    else return 1;
  }
  int destroy_rc = turbo_flow_plugin_host_destroy(host, CONSUMER_TIMEOUT_MS, &error);
  return rc == SALTS_OK && destroy_rc == SALTS_OK ? 0 : 1;
}
