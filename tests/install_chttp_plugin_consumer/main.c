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

static int run_kind(turbo_flow_plugin_host_t *host, size_t kind) {
  static const char *const yamls[] = {client_yaml, server_yaml, websocket_yaml};
  static const char *const graphs[] = {"source input\nstage request adapter client\nstage "
                                       "output\nstage main {\n input -> request -> output\n}\n",
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
    rc = turbo_flow_register_stage_ex(flow, "output", discard_response, NULL, NULL);
    if (rc != SALTS_OK) goto cleanup;
  }
  rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, &generation, &ce);
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
  if (generation) {
    int destroy_rc = turbo_flow_plugin_generation_destroy(generation, CONSUMER_TIMEOUT_MS, &ce);
    if (destroy_rc != SALTS_OK) rc = destroy_rc;
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
    rc = run_kind(host, i);
  int destroy_rc = turbo_flow_plugin_host_destroy(host, CONSUMER_TIMEOUT_MS, &error);
  return rc == SALTS_OK && destroy_rc == SALTS_OK ? 0 : 1;
}
