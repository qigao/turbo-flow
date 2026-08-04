#include "flowie_server_application_internal.h"

#include "flowie.h"
#include "flowie_cluster_generation_internal.h"
#include "flowie_control_config_internal.h"
#include "flowie_control_runtime_internal.h"
#include "socket.h"
#include "turbo_error.h"
#include "turbo_flow_http_acl.h"
#include "turbo_flow_http_auth.h"
#include "turbo_flow_http_client.h"
#include "turbo_flow_http_server.h"
#include "turbo_flow_local_storage_backend.h"
#include "turbo_flow_pgsql.h"
#include "turbo_flow_pgsql_storage_backend.h"
#include "turbo_flow_redis.h"
#include "turbo_flow_redis_storage_backend.h"
#include "turbo_flow_sqlite_storage_backend.h"
#include "turbo_flow_storage_backend.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWIE_SERVER_STORAGE_BACKEND_MAX = 32u };

struct flowie_server_application_s {
  turbo_flow_storage_backend_registry_t *storage_backends;
  flowie_worker_runtime_t *worker;
  flowie_control_config_t control_config;
  flowie_control_runtime_t *control;
  flowie_cluster_generation_t *cluster;
  int control_enabled;
  int started;
};

static void flowie_server_application_error_reset(flowie_server_application_error_t *error) {
  if (!error || error->size != sizeof(*error)) return;
  *error = (flowie_server_application_error_t)FLOWIE_SERVER_APPLICATION_ERROR_INIT;
}

static void flowie_server_application_error_set(flowie_server_application_error_t *error,
                                                const char *operation, int status) {
  if (!error || error->size != sizeof(*error)) return;
  error->operation = operation;
  error->status = status;
}

static void flowie_server_application_error_set_worker(flowie_server_application_error_t *error,
                                                       const flowie_worker_error_t *worker) {
  if (!error || error->size != sizeof(*error) || !worker) return;
  error->operation = worker->operation;
  error->status = worker->status;
  error->detail = FLOWIE_SERVER_APPLICATION_ERROR_WORKER;
  error->worker = *worker;
}

static void flowie_server_application_error_set_control_config(
    flowie_server_application_error_t *error, const flowie_control_config_error_t *control_error) {
  if (!error || error->size != sizeof(*error) || !control_error) return;
  error->operation = "load control configuration";
  error->status = control_error->status;
  error->detail = FLOWIE_SERVER_APPLICATION_ERROR_CONTROL_CONFIG;
  (void)snprintf(error->subject, sizeof(error->subject), "%s",
                 control_error->path[0] ? control_error->path : "$");
  (void)snprintf(error->message, sizeof(error->message), "%s",
                 control_error->message[0] ? control_error->message
                                           : "invalid control configuration");
}

static void flowie_server_application_error_set_control(flowie_server_application_error_t *error,
                                                        const char *operation, int status) {
  flowie_server_application_error_set(error, operation, status);
  if (error && error->size == sizeof(*error))
    error->detail = FLOWIE_SERVER_APPLICATION_ERROR_CONTROL_RUNTIME;
}

static void flowie_server_application_error_set_cluster(flowie_server_application_error_t *error,
                                                        const char *operation, int status) {
  flowie_server_application_error_set(error, operation, status);
  if (error && error->size == sizeof(*error))
    error->detail = FLOWIE_SERVER_APPLICATION_ERROR_CLUSTER_GENERATION;
}

static int flowie_server_register_socket_adapter(void *ctx, turbo_flow_t *flow,
                                                 const turbo_flow_resolved_config_t *resolved,
                                                 const char *adapter_name,
                                                 turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)error;
  return turbo_flow_coronet_register_socket_resolved_adapter(flow, resolved, adapter_name);
}

static int flowie_server_register_redis_adapter(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *adapter_name,
                                                turbo_flow_config_error_t *error) {
  (void)ctx;
  return turbo_flow_redis_register_resolved_adapter(flow, adapter_name, resolved, error);
}

static int flowie_server_register_pgsql_adapter(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *adapter_name,
                                                turbo_flow_config_error_t *error) {
  (void)ctx;
  return turbo_flow_pgsql_register_resolved_outbox_adapter(flow, adapter_name, resolved, error);
}

static int flowie_server_register_mqtt_client_adapter(void *ctx, turbo_flow_t *flow,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      const char *adapter_name,
                                                      turbo_flow_config_error_t *error) {
  (void)ctx;
  return flowie_register_resolved_client_source(flow, adapter_name, resolved, error);
}

static int flowie_server_register_http_adapter(void *ctx, turbo_flow_t *flow,
                                               const turbo_flow_resolved_config_t *resolved,
                                               const char *adapter_name,
                                               turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  const char *url = NULL;
  int rc;
  (void)ctx;
  (void)error;
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_resolved_adapter_get_string(&view, "url", &url);
  if (rc == TURBO_OK)
    return turbo_flow_http_register_client_resolved_adapter(flow, resolved, adapter_name);
  if (rc != TURBO_ENOENT) return rc;
  return turbo_flow_http_register_server_resolved_adapter(flow, resolved, adapter_name);
}

static int
flowie_server_register_builtin_storage_backends(turbo_flow_storage_backend_registry_t *registry) {
  int rc;
  if (!registry) return TURBO_EINVAL;
  rc = turbo_flow_storage_backend_registry_register(registry,
                                                    turbo_flow_local_storage_backend_api());
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_registry_register(registry,
                                                    turbo_flow_redis_storage_backend_api());
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_registry_register(registry,
                                                    turbo_flow_pgsql_storage_backend_api());
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_registry_register(registry,
                                                    turbo_flow_sqlite_storage_backend_api());
  if (rc != TURBO_OK) return rc;
  return TURBO_OK;
}

int flowie_server_application_create(const flowie_server_application_config_t *config,
                                     flowie_server_application_t **out,
                                     flowie_server_application_error_t *error) {
  turbo_flow_product_adapter_provider_t adapter_providers[5] = {
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT, TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT,
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT, TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT,
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT};
  size_t adapter_provider_count = 0u;
  flowie_worker_runtime_config_t worker_config = FLOWIE_WORKER_RUNTIME_CONFIG_INIT;
  flowie_worker_error_t worker_error = FLOWIE_WORKER_ERROR_INIT;
  flowie_control_config_error_t control_error = FLOWIE_CONTROL_CONFIG_ERROR_INIT;
  const turbo_flow_security_auth_provider_factory_t *auth_provider_factories[] = {
      turbo_flow_http_auth_provider_factory()};
  const turbo_flow_security_policy_provider_factory_t *policy_provider_factories[] = {
      turbo_flow_http_acl_provider_factory()};
  char plugin_error[FLOWIE_SERVER_APPLICATION_ERROR_MESSAGE_MAX];
  flowie_server_application_t *application = NULL;
  int rc;

  if (out) *out = NULL;
  flowie_server_application_error_reset(error);
  if (!config || config->size != sizeof(*config) || !out || !config->profile ||
      !config->profile[0] || !config->config_path || !config->config_path[0] ||
      !config->graph_path || !config->graph_path[0] ||
      (config->protocol_store_path &&
       strcmp(config->protocol_store_path, FLOWIE_SERVER_DEFAULT_PROTOCOL_STORE_PATH) != 0) ||
      (config->control_config_path && !config->control_config_path[0]) ||
      (config->endpoint_cluster && !config->endpoint_execution) ||
      (config->cluster && (config->endpoint_execution || config->endpoint_cluster)) ||
      config->storage_backend_plugin_path_count > FLOWIE_SERVER_STORAGE_BACKEND_PLUGIN_MAX ||
      (config->storage_backend_plugin_path_count > 0u && !config->storage_backend_plugin_paths)) {
    flowie_server_application_error_set(error, "validate server configuration", TURBO_EINVAL);
    return TURBO_EINVAL;
  }
  for (size_t i = 0u; i < config->storage_backend_plugin_path_count; ++i) {
    if (!config->storage_backend_plugin_paths[i] || !config->storage_backend_plugin_paths[i][0]) {
      flowie_server_application_error_set(error, "validate storage backend plugin", TURBO_EINVAL);
      return TURBO_EINVAL;
    }
  }

  application = (flowie_server_application_t *)calloc(1u, sizeof(*application));
  if (!application) {
    flowie_server_application_error_set(error, "create server application", TURBO_ENOMEM);
    return TURBO_ENOMEM;
  }
  application->control_config = (flowie_control_config_t)FLOWIE_CONTROL_CONFIG_INIT;
  if (config->control_config_path) {
    rc = flowie_control_config_load(config->control_config_path, &application->control_config,
                                    &control_error);
    if (rc != TURBO_OK) {
      flowie_server_application_error_set_control_config(error, &control_error);
      goto fail;
    }
    rc = flowie_control_runtime_validate(&application->control_config);
    if (rc != TURBO_OK) {
      flowie_server_application_error_set_control(error, "validate control runtime", rc);
      goto fail;
    }
    application->control_enabled = 1;
  }
  rc = turbo_flow_storage_backend_registry_create(FLOWIE_SERVER_STORAGE_BACKEND_MAX,
                                                  &application->storage_backends);
  if (rc == TURBO_OK)
    rc = flowie_server_register_builtin_storage_backends(application->storage_backends);
  if (rc != TURBO_OK) {
    flowie_server_application_error_set(error, "configure storage backends", rc);
    goto fail;
  }
  for (size_t i = 0u; i < config->storage_backend_plugin_path_count; ++i) {
    plugin_error[0] = '\0';
    rc = turbo_flow_storage_backend_registry_load(application->storage_backends,
                                                  config->storage_backend_plugin_paths[i],
                                                  plugin_error, sizeof(plugin_error));
    if (rc != TURBO_OK) {
      flowie_server_application_error_set(error, "load storage backend plugin", rc);
      if (error && error->size == sizeof(*error)) {
        error->detail = FLOWIE_SERVER_APPLICATION_ERROR_STORAGE_PLUGIN;
        (void)snprintf(error->subject, sizeof(error->subject), "%s",
                       config->storage_backend_plugin_paths[i]);
        (void)snprintf(error->message, sizeof(error->message), "%s",
                       plugin_error[0] ? plugin_error : "failed to load storage backend plugin");
      }
      goto fail;
    }
  }

  adapter_providers[adapter_provider_count].kind = "socket";
  adapter_providers[adapter_provider_count++].register_adapter =
      flowie_server_register_socket_adapter;
  adapter_providers[adapter_provider_count].kind = "redis";
  adapter_providers[adapter_provider_count++].register_adapter =
      flowie_server_register_redis_adapter;
  adapter_providers[adapter_provider_count].kind = "pgsql_outbox";
  adapter_providers[adapter_provider_count++].register_adapter =
      flowie_server_register_pgsql_adapter;
  adapter_providers[adapter_provider_count].kind = "flowie_client";
  adapter_providers[adapter_provider_count++].register_adapter =
      flowie_server_register_mqtt_client_adapter;
  adapter_providers[adapter_provider_count].kind = "http";
  adapter_providers[adapter_provider_count++].register_adapter =
      flowie_server_register_http_adapter;

  worker_config.profile = config->profile;
  worker_config.config_path = config->config_path;
  worker_config.graph_path = config->graph_path;
  worker_config.protocol_store_path = config->protocol_store_path
                                          ? config->protocol_store_path
                                          : FLOWIE_SERVER_DEFAULT_PROTOCOL_STORE_PATH;
  worker_config.storage_backends = application->storage_backends;
  worker_config.adapter_providers = adapter_providers;
  worker_config.adapter_provider_count = adapter_provider_count;
  worker_config.require_security = config->require_security;
  worker_config.endpoint_execution = config->endpoint_execution;
  worker_config.endpoint_cluster = config->endpoint_cluster;
  worker_config.endpoint_proxy = config->endpoint_proxy;
  worker_config.auth_provider_factories = auth_provider_factories;
  worker_config.auth_provider_factory_count =
      sizeof(auth_provider_factories) / sizeof(auth_provider_factories[0]);
  worker_config.policy_provider_factories = policy_provider_factories;
  worker_config.policy_provider_factory_count =
      sizeof(policy_provider_factories) / sizeof(policy_provider_factories[0]);
  if (config->cluster) {
    rc = flowie_cluster_generation_create(config->cluster, &application->cluster);
    if (rc != TURBO_OK) {
      flowie_server_application_error_set_cluster(error, "create cluster generation", rc);
      goto fail;
    }
    worker_config.endpoint_execution =
        flowie_cluster_generation_endpoint_execution(application->cluster);
    worker_config.endpoint_cluster = flowie_cluster_generation_endpoint_port(application->cluster);
    if (!worker_config.endpoint_execution || !worker_config.endpoint_cluster) {
      rc = TURBO_EINVAL;
      flowie_server_application_error_set_cluster(error, "bind cluster generation", rc);
      goto fail;
    }
  }
  rc = flowie_worker_runtime_create(&worker_config, &application->worker, &worker_error);
  if (rc != TURBO_OK) {
    flowie_server_application_error_set_worker(error, &worker_error);
    goto fail;
  }
  *out = application;
  return TURBO_OK;

fail:
  (void)flowie_worker_runtime_destroy(application->worker, NULL);
  (void)flowie_cluster_generation_destroy(application->cluster);
  (void)turbo_flow_storage_backend_registry_destroy(application->storage_backends);
  free(application);
  return rc;
}

int flowie_server_application_start(flowie_server_application_t *application,
                                    flowie_server_application_error_t *error) {
  flowie_worker_error_t worker_error = FLOWIE_WORKER_ERROR_INIT;
  int rc;
  flowie_server_application_error_reset(error);
  if (!application || !application->worker || application->started) {
    flowie_server_application_error_set(error, "validate server start", TURBO_EINVAL);
    return TURBO_EINVAL;
  }
  if (application->cluster) {
    rc = flowie_cluster_generation_start(application->cluster);
    if (rc != TURBO_OK) {
      flowie_server_application_error_set_cluster(error, "start cluster generation", rc);
      return rc;
    }
  }
  if (application->control_enabled) {
    rc = flowie_control_runtime_create(&application->control_config, &application->control);
    if (rc != TURBO_OK) {
      flowie_server_application_error_set_control(error, "create control runtime", rc);
      if (application->cluster) (void)flowie_cluster_generation_close(application->cluster);
      return rc;
    }
    rc = flowie_control_runtime_start(application->control);
    if (rc != TURBO_OK) {
      int cleanup_rc = flowie_control_runtime_destroy(application->control);
      if (cleanup_rc == TURBO_OK) application->control = NULL;
      flowie_server_application_error_set_control(error, "start control listener", rc);
      if (application->cluster) (void)flowie_cluster_generation_close(application->cluster);
      return rc;
    }
  }
  rc = flowie_worker_runtime_start(application->worker, &worker_error);
  if (rc != TURBO_OK) {
    if (application->control) {
      (void)flowie_control_runtime_stop(application->control);
      if (flowie_control_runtime_destroy(application->control) == TURBO_OK)
        application->control = NULL;
    }
    if (application->cluster) (void)flowie_cluster_generation_close(application->cluster);
    flowie_server_application_error_set_worker(error, &worker_error);
    return rc;
  }
  application->started = 1;
  return TURBO_OK;
}

int flowie_server_application_stop(flowie_server_application_t *application,
                                   flowie_server_application_error_t *error) {
  flowie_worker_error_t worker_error = FLOWIE_WORKER_ERROR_INIT;
  int result = TURBO_OK;
  int worker_stopped = 0;
  int rc;
  flowie_server_application_error_reset(error);
  if (!application || !application->worker) {
    flowie_server_application_error_set(error, "validate server stop", TURBO_EINVAL);
    return TURBO_EINVAL;
  }
  if (!application->started) return TURBO_OK;
  rc = flowie_worker_runtime_stop(application->worker, &worker_error);
  if (rc != TURBO_OK) {
    result = rc;
    flowie_server_application_error_set_worker(error, &worker_error);
  } else worker_stopped = 1;
  if (application->control) {
    rc = flowie_control_runtime_stop(application->control);
    if (rc != TURBO_OK && result == TURBO_OK) {
      result = rc;
      flowie_server_application_error_set_control(error, "stop control listener", rc);
    }
  }
  if (application->cluster && worker_stopped) {
    rc = flowie_cluster_generation_close(application->cluster);
    if (rc != TURBO_OK && result == TURBO_OK) {
      result = rc;
      flowie_server_application_error_set_cluster(error, "close cluster generation", rc);
    }
  }
  if (result == TURBO_OK) application->started = 0;
  return result;
}

int flowie_server_application_destroy(flowie_server_application_t *application,
                                      flowie_server_application_error_t *error) {
  flowie_worker_error_t worker_error = FLOWIE_WORKER_ERROR_INIT;
  int result = TURBO_OK;
  int rc;
  flowie_server_application_error_reset(error);
  if (!application) return TURBO_OK;
  rc = flowie_worker_runtime_destroy(application->worker, &worker_error);
  application->worker = NULL;
  if (rc != TURBO_OK) {
    result = rc;
    flowie_server_application_error_set_worker(error, &worker_error);
  }
  rc = flowie_control_runtime_destroy(application->control);
  if (rc != TURBO_OK) {
    if (result == TURBO_OK) {
      result = rc;
      flowie_server_application_error_set_control(error, "destroy control runtime", rc);
    }
    return result;
  }
  application->control = NULL;
  rc = flowie_cluster_generation_destroy(application->cluster);
  if (rc != TURBO_OK) {
    if (result == TURBO_OK) {
      result = rc;
      flowie_server_application_error_set_cluster(error, "destroy cluster generation", rc);
    }
    return result;
  }
  application->cluster = NULL;
  rc = turbo_flow_storage_backend_registry_destroy(application->storage_backends);
  application->storage_backends = NULL;
  if (rc != TURBO_OK && result == TURBO_OK) {
    result = rc;
    flowie_server_application_error_set(error, "destroy storage backend registry", rc);
  }
  free(application);
  return result;
}
