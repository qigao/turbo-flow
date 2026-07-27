#include "flowie_worker_runtime_internal.h"

#include "socket.h"
#include "turbo_error.h"
#include "turbo_flow_storage_backend.h"
#include "turbo_flow_local_storage_backend.h"
#include "turbo_thread.h"
#ifdef FLOWIE_SERVER_HAVE_REDIS
  #include "turbo_flow_redis.h"
  #include "turbo_flow_redis_storage_backend.h"
#endif
#ifdef FLOWIE_SERVER_HAVE_HTTP_CLIENT
  #include "turbo_flow_http_client.h"
#endif
#ifdef FLOWIE_SERVER_HAVE_HTTP_SERVER
  #include "turbo_flow_http_server.h"
#endif
#ifdef FLOWIE_SERVER_HAVE_PGSQL
  #include "turbo_flow_pgsql.h"
  #include "turbo_flow_pgsql_storage_backend.h"
#endif
#ifdef FLOWIE_SERVER_HAVE_HTTPS_AUTH
  #include "turbo_flow_http_acl.h"
  #include "turbo_flow_http_auth.h"
#endif
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_STORAGE_BACKEND_MAX 32u
#define FLOWIE_STORAGE_BACKEND_PLUGIN_MAX 8u
#define FLOWIE_STORAGE_BACKEND_ERROR_MAX 512u

static volatile sig_atomic_t flowie_server_stop_requested = 0;

static void flowie_server_signal(int signal_number) {
  (void)signal_number;
  flowie_server_stop_requested = 1;
}

static int flowie_server_register_socket_adapter(void *ctx, turbo_flow_t *flow,
                                                 const turbo_flow_resolved_config_t *resolved,
                                                 const char *adapter_name,
                                                 turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)error;
  return turbo_flow_coronet_register_socket_resolved_adapter(flow, resolved, adapter_name);
}

#ifdef FLOWIE_SERVER_HAVE_REDIS
static int flowie_server_register_redis_adapter(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *adapter_name,
                                                turbo_flow_config_error_t *error) {
  (void)ctx;
  return turbo_flow_redis_register_resolved_adapter(flow, adapter_name, resolved, error);
}
#endif

#ifdef FLOWIE_SERVER_HAVE_PGSQL
static int flowie_server_register_pgsql_adapter(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *adapter_name,
                                                turbo_flow_config_error_t *error) {
  (void)ctx;
  return turbo_flow_pgsql_register_resolved_outbox_adapter(flow, adapter_name, resolved, error);
}
#endif

#if defined(FLOWIE_SERVER_HAVE_HTTP_CLIENT) || defined(FLOWIE_SERVER_HAVE_HTTP_SERVER)
static int flowie_server_register_http_adapter(void *ctx, turbo_flow_t *flow,
                                               const turbo_flow_resolved_config_t *resolved,
                                               const char *adapter_name,
                                               turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  const char *url = NULL;
  int rc;
  (void)ctx;
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_resolved_adapter_get_string(&view, "url", &url);
  if (rc == TURBO_OK) {
#ifdef FLOWIE_SERVER_HAVE_HTTP_CLIENT
    return turbo_flow_http_register_client_resolved_adapter(flow, resolved, adapter_name);
#else
    return TURBO_ENOTSUP;
#endif
  }
  if (rc != TURBO_ENOENT) return rc;
#ifdef FLOWIE_SERVER_HAVE_HTTP_SERVER
  return turbo_flow_http_register_server_resolved_adapter(flow, resolved, adapter_name);
#else
  if (error && error->size >= sizeof(*error)) {
    error->status = TURBO_ENOTSUP;
    (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.kind", adapter_name);
    (void)snprintf(error->message, sizeof(error->message),
                   "HTTP server adapter is disabled in this host build");
  }
  return TURBO_ENOTSUP;
#endif
}
#endif

static void flowie_server_usage(const char *program) {
  (void)fprintf(
      stderr,
      "Usage: %s [--check] [--profile NAME] [--storage-backend-plugin PATH] <config.yml> "
      "<graph.flow>\n",
      program ? program : "flowie_server");
}

static int flowie_server_report(const flowie_worker_error_t *error) {
  const char *operation = error && error->operation ? error->operation : "worker operation";
  int status = error ? error->status : TURBO_EIO;
  if (error && error->detail == FLOWIE_WORKER_ERROR_CONFIG && error->config.status != TURBO_OK) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d path=%s message=%s\n", operation,
                  status, error->config.path[0] ? error->config.path : "-",
                  error->config.message[0] ? error->config.message : "-");
  } else if (error && error->detail == FLOWIE_WORKER_ERROR_FLOW &&
             error->flow.code != TURBO_OK) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d line=%u column=%u message=%s\n",
                  operation, status, error->flow.line, error->flow.column,
                  error->flow.message[0] ? error->flow.message : "-");
  } else {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d\n", operation, status);
  }
  return EXIT_FAILURE;
}

static int flowie_server_register_builtin_storage_backends(
    turbo_flow_storage_backend_registry_t *registry) {
  int rc;
  if (!registry) return TURBO_EINVAL;
  rc = turbo_flow_storage_backend_registry_register(
      registry, turbo_flow_local_storage_backend_api());
  if (rc != TURBO_OK) return rc;
#ifdef FLOWIE_SERVER_HAVE_REDIS
  rc = turbo_flow_storage_backend_registry_register(
      registry, turbo_flow_redis_storage_backend_api());
  if (rc != TURBO_OK) return rc;
#endif
#ifdef FLOWIE_SERVER_HAVE_PGSQL
  rc = turbo_flow_storage_backend_registry_register(
      registry, turbo_flow_pgsql_storage_backend_api());
  if (rc != TURBO_OK) return rc;
#endif
  return TURBO_OK;
}

int main(int argc, char **argv) {
  flowie_worker_runtime_config_t config = FLOWIE_WORKER_RUNTIME_CONFIG_INIT;
  turbo_flow_product_adapter_provider_t adapter_providers[4] = {
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT, TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT,
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT, TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT};
  size_t adapter_provider_count = 0u;
#ifdef FLOWIE_SERVER_HAVE_HTTPS_AUTH
  const turbo_flow_security_auth_provider_factory_t *auth_provider_factories[] = {
      turbo_flow_http_auth_provider_factory()};
#endif
  const turbo_flow_security_policy_provider_factory_t *policy_provider_factories[1];
  size_t policy_provider_factory_count = 0u;
  const char *storage_backend_plugin_paths[FLOWIE_STORAGE_BACKEND_PLUGIN_MAX];
  size_t storage_backend_plugin_path_count = 0u;
  turbo_flow_storage_backend_registry_t *storage_backends = NULL;
  char storage_backend_error[FLOWIE_STORAGE_BACKEND_ERROR_MAX];
  flowie_worker_error_t error = FLOWIE_WORKER_ERROR_INIT;
  flowie_worker_runtime_t *runtime = NULL;
  int check_only = 0;
  int result = EXIT_FAILURE;
  int rc;

  adapter_providers[adapter_provider_count].kind = "socket";
  adapter_providers[adapter_provider_count].register_adapter =
      flowie_server_register_socket_adapter;
  ++adapter_provider_count;
#ifdef FLOWIE_SERVER_HAVE_REDIS
  adapter_providers[adapter_provider_count].kind = "redis";
  adapter_providers[adapter_provider_count].register_adapter = flowie_server_register_redis_adapter;
  ++adapter_provider_count;
#endif
#ifdef FLOWIE_SERVER_HAVE_PGSQL
  adapter_providers[adapter_provider_count].kind = "pgsql_outbox";
  adapter_providers[adapter_provider_count].register_adapter = flowie_server_register_pgsql_adapter;
  ++adapter_provider_count;
#endif
#if defined(FLOWIE_SERVER_HAVE_HTTP_CLIENT) || defined(FLOWIE_SERVER_HAVE_HTTP_SERVER)
  adapter_providers[adapter_provider_count].kind = "http";
  adapter_providers[adapter_provider_count].register_adapter = flowie_server_register_http_adapter;
  ++adapter_provider_count;
#endif
  config.adapter_providers = adapter_providers;
  config.adapter_provider_count = adapter_provider_count;

#ifdef FLOWIE_SERVER_HAVE_HTTPS_AUTH
  config.auth_provider_factories = auth_provider_factories;
  config.auth_provider_factory_count =
      sizeof(auth_provider_factories) / sizeof(auth_provider_factories[0]);
#endif
#ifdef FLOWIE_SERVER_HAVE_HTTPS_AUTH
  policy_provider_factories[policy_provider_factory_count++] =
      turbo_flow_http_acl_provider_factory();
#endif
  config.policy_provider_factories = policy_provider_factories;
  config.policy_provider_factory_count = policy_provider_factory_count;

  for (int index = 1; index < argc; ++index) {
    if (strcmp(argv[index], "--check") == 0) {
      check_only = 1;
    } else if (strcmp(argv[index], "--profile") == 0) {
      if (++index >= argc || !argv[index][0]) {
        flowie_server_usage(argv[0]);
        goto done;
      }
      config.profile = argv[index];
    } else if (strcmp(argv[index], "--storage-backend-plugin") == 0 ||
               strcmp(argv[index], "--record-store-plugin") == 0) {
      if (++index >= argc || !argv[index][0] ||
          storage_backend_plugin_path_count >= FLOWIE_STORAGE_BACKEND_PLUGIN_MAX) {
        flowie_server_usage(argv[0]);
        goto done;
      }
      storage_backend_plugin_paths[storage_backend_plugin_path_count++] = argv[index];
    } else if (strcmp(argv[index], "--help") == 0 || strcmp(argv[index], "-h") == 0) {
      flowie_server_usage(argv[0]);
      result = EXIT_SUCCESS;
      goto done;
    } else if (!config.config_path) {
      config.config_path = argv[index];
    } else if (!config.graph_path) {
      config.graph_path = argv[index];
    } else {
      flowie_server_usage(argv[0]);
      goto done;
    }
  }
  if (!config.config_path || !config.graph_path) {
    flowie_server_usage(argv[0]);
    goto done;
  }

  rc = turbo_flow_storage_backend_registry_create(FLOWIE_STORAGE_BACKEND_MAX, &storage_backends);
  if (rc == TURBO_OK) rc = flowie_server_register_builtin_storage_backends(storage_backends);
  if (rc != TURBO_OK) {
    error.operation = "configure storage backends";
    error.status = rc;
    result = flowie_server_report(&error);
    goto done;
  }
  for (size_t i = 0u; i < storage_backend_plugin_path_count; ++i) {
    rc = turbo_flow_storage_backend_registry_load(
        storage_backends, storage_backend_plugin_paths[i], storage_backend_error,
        sizeof(storage_backend_error));
    if (rc != TURBO_OK) {
      (void)fprintf(stderr, "flowie_server: %s: %s\n", storage_backend_plugin_paths[i],
                    storage_backend_error[0] ? storage_backend_error
                                             : "failed to load storage backend plugin");
      error.operation = "load storage backend plugin";
      error.status = rc;
      result = flowie_server_report(&error);
      goto done;
    }
  }
  config.storage_backends = storage_backends;

  rc = flowie_worker_runtime_create(&config, &runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  if (check_only) {
    (void)fprintf(stdout, "flowie_server: configuration and graph are valid\n");
    result = EXIT_SUCCESS;
    goto done;
  }
  if (signal(SIGINT, flowie_server_signal) == SIG_ERR ||
      signal(SIGTERM, flowie_server_signal) == SIG_ERR) {
    error.operation = "install signal handlers";
    error.status = TURBO_EIO;
    error.detail = FLOWIE_WORKER_ERROR_NONE;
    result = flowie_server_report(&error);
    goto done;
  }
  rc = flowie_worker_runtime_start(runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  (void)fprintf(stdout, "flowie_server: running; press Ctrl+C to stop\n");
  while (!flowie_server_stop_requested)
    turbo_sleep_ms(100u);
  rc = flowie_worker_runtime_stop(runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  result = EXIT_SUCCESS;

done:
  rc = flowie_worker_runtime_destroy(runtime, &error);
  if (rc != TURBO_OK && result == EXIT_SUCCESS) result = flowie_server_report(&error);
  rc = turbo_flow_storage_backend_registry_destroy(storage_backends);
  if (rc != TURBO_OK && result == EXIT_SUCCESS) {
    error.operation = "destroy storage backend registry";
    error.status = rc;
    result = flowie_server_report(&error);
  }
  return result;
}
