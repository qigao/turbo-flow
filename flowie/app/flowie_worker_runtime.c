#include "flowie_worker_runtime_internal.h"

#include "flowie.h"
#include "socket.h"
#include "turbo_flow_queue.h"
#ifdef FLOWIE_SERVER_HAVE_REDIS
  #include "turbo_flow_redis.h"
#endif

#include "turbo_error.h"
#include "turbo_fs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum flowie_worker_store_provider_e {
  FLOWIE_WORKER_STORE_NONE = 0,
  FLOWIE_WORKER_STORE_SQLITE,
  FLOWIE_WORKER_STORE_REDIS
} flowie_worker_store_provider_t;

struct flowie_worker_runtime_s {
  turbo_fs_buf_t yaml;
  turbo_fs_buf_t graph;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_queue_t *queue;
  turbo_flow_record_store_t session_store;
  flowie_worker_store_provider_t session_store_provider;
  turbo_flow_rule_processor_t *rule_processor;
  turbo_flow_t *flow;
  int started;
};

static void flowie_worker_error_reset(flowie_worker_error_t *error) {
  if (!error || error->size != sizeof(*error)) return;
  *error = (flowie_worker_error_t)FLOWIE_WORKER_ERROR_INIT;
}

static void flowie_worker_error_set(flowie_worker_error_t *error, const char *operation, int status,
                                    const turbo_flow_config_error_t *config_error,
                                    const turbo_flow_error_t *flow_error) {
  if (!error || error->size != sizeof(*error)) return;
  flowie_worker_error_reset(error);
  error->operation = operation;
  error->status = status;
  if (config_error && config_error->status != TURBO_OK) {
    error->detail = FLOWIE_WORKER_ERROR_CONFIG;
    error->config = *config_error;
  } else if (flow_error && flow_error->code != TURBO_OK) {
    error->detail = FLOWIE_WORKER_ERROR_FLOW;
    error->flow = *flow_error;
  }
}

static int flowie_worker_resolve_queue_channel(const turbo_flow_resolved_config_t *resolved,
                                               const char *adapter_name, const char **channel,
                                               turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  int rc;
  if (!resolved || !adapter_name || !channel || !error) return TURBO_EINVAL;
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc == TURBO_OK && strcmp(view.kind, "queue") != 0) rc = TURBO_EINVAL;
  if (rc == TURBO_OK) rc = turbo_flow_resolved_adapter_get_string(&view, "channel", channel);
  if (rc != TURBO_OK) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = rc;
    (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.channel", adapter_name);
    (void)snprintf(error->message, sizeof(error->message),
                   "profile accept_sink must reference a Queue adapter with a channel");
  }
  return rc;
}

static int flowie_worker_resolve_session_store(const turbo_flow_resolved_config_t *resolved,
                                               const char *endpoint_name, const char **channel,
                                               turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  int rc;
  if (channel) *channel = NULL;
  if (!resolved || !endpoint_name || !endpoint_name[0] || !channel || !error) return TURBO_EINVAL;
  rc = turbo_flow_resolved_config_adapter(resolved, endpoint_name, &view);
  if (rc == TURBO_OK && strcmp(view.kind, "flowie_endpoint") != 0) rc = TURBO_EINVAL;
  if (rc == TURBO_OK) rc = turbo_flow_resolved_adapter_get_string(&view, "session_store", channel);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc == TURBO_OK && *channel && (*channel)[0]) return TURBO_OK;
  *channel = NULL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  error->status = rc == TURBO_OK ? TURBO_EINVAL : rc;
  (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.session_store",
                 endpoint_name);
  (void)snprintf(error->message, sizeof(error->message),
                 "session_store must be a non-empty record-store channel name");
  return error->status;
}

static int flowie_worker_create_session_store(const turbo_flow_resolved_config_t *resolved,
                                              const char *channel, turbo_flow_record_store_t *store,
                                              flowie_worker_store_provider_t *provider,
                                              turbo_flow_config_error_t *error) {
  turbo_flow_config_error_t sqlite_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  int rc;
  if (provider) *provider = FLOWIE_WORKER_STORE_NONE;
  if (!resolved || !channel || !channel[0] || !store || !provider || !error) return TURBO_EINVAL;
  rc = turbo_flow_sqlite_record_store_create_resolved(resolved, channel, store, &sqlite_error);
  if (rc == TURBO_OK) {
    *provider = FLOWIE_WORKER_STORE_SQLITE;
    return TURBO_OK;
  }
  if (rc != TURBO_ENOTSUP) {
    *error = sqlite_error;
    return rc;
  }
#ifdef FLOWIE_SERVER_HAVE_REDIS
  {
    turbo_flow_config_error_t redis_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    rc = turbo_flow_redis_record_store_create_resolved(resolved, channel, store, &redis_error);
    if (rc == TURBO_OK) {
      *provider = FLOWIE_WORKER_STORE_REDIS;
      return TURBO_OK;
    }
    if (rc != TURBO_ENOTSUP) {
      *error = redis_error;
      return rc;
    }
  }
#endif
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  error->status = TURBO_ENOTSUP;
  (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.backend", channel);
  (void)snprintf(error->message, sizeof(error->message),
                 "flowie_server supports SQLite and Redis session record stores");
  return TURBO_ENOTSUP;
}

static void flowie_worker_destroy_session_store(turbo_flow_record_store_t *store,
                                                flowie_worker_store_provider_t provider) {
  if (!store || !store->ctx) return;
  if (provider == FLOWIE_WORKER_STORE_SQLITE) {
    turbo_flow_sqlite_record_store_destroy(store);
  }
#ifdef FLOWIE_SERVER_HAVE_REDIS
  else if (provider == FLOWIE_WORKER_STORE_REDIS) {
    turbo_flow_redis_record_store_destroy(store);
  }
#endif
}

int flowie_worker_runtime_create(const flowie_worker_runtime_config_t *config,
                                 flowie_worker_runtime_t **out, flowie_worker_error_t *error) {
  static const char *const enabled_kinds[] = {"flowie_endpoint", "queue", "socket"};
  const char *endpoint_name = NULL;
  const char *accept_sink_name = NULL;
  const char *accepted_source_name = NULL;
  const char *rule_set_channel = NULL;
  const char *output_name = NULL;
  const char *queue_channel = NULL;
  const char *session_store_channel = NULL;
  const char *failure_operation = NULL;
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_async_ingress_config_t ingress_config = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  const turbo_flow_error_t *flow_error = NULL;
  flowie_worker_runtime_t *runtime;
  int rc;

  if (out) *out = NULL;
  flowie_worker_error_reset(error);
  if (!config || config->size != sizeof(*config) || !out || !config->profile ||
      !config->profile[0] || !config->config_path || !config->config_path[0] ||
      !config->graph_path || !config->graph_path[0]) {
    flowie_worker_error_set(error, "validate worker configuration", TURBO_EINVAL, NULL, NULL);
    return TURBO_EINVAL;
  }
  runtime = (flowie_worker_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) {
    flowie_worker_error_set(error, "create worker", TURBO_ENOMEM, NULL, NULL);
    return TURBO_ENOMEM;
  }
  runtime->session_store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;

  rc = turbo_fs_read_file(config->config_path, &runtime->yaml);
  if (rc != TURBO_OK) {
    failure_operation = "read config";
    goto fail;
  }
  rc = turbo_fs_read_file(config->graph_path, &runtime->graph);
  if (rc != TURBO_OK) {
    failure_operation = "read graph";
    goto fail;
  }
  rc = turbo_flow_config_resolve_yaml(runtime->yaml.base, runtime->yaml.len, &runtime->resolved,
                                      &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "resolve config";
    goto fail;
  }
  rc = turbo_flow_resolved_config_runtime_ingress(runtime->resolved, &ingress_config);
  if (rc != TURBO_OK) {
    failure_operation = "resolve runtime ingress";
    goto fail;
  }
  rc = turbo_flow_resolved_config_preflight_adapter_kinds(
      runtime->resolved, enabled_kinds, sizeof(enabled_kinds) / sizeof(enabled_kinds[0]),
      &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "preflight adapter kinds";
    goto fail;
  }
  rc = turbo_flow_resolved_config_profile_adapter(runtime->resolved, config->profile, "endpoint",
                                                  &endpoint_name);
  if (rc == TURBO_OK)
    rc = turbo_flow_resolved_config_profile_adapter(runtime->resolved, config->profile,
                                                    "accept_sink", &accept_sink_name);
  if (rc == TURBO_OK)
    rc = turbo_flow_resolved_config_profile_adapter(runtime->resolved, config->profile,
                                                    "accepted_source", &accepted_source_name);
  if (rc == TURBO_OK)
    rc = turbo_flow_resolved_config_profile_channel(runtime->resolved, config->profile, "rule_set",
                                                    &rule_set_channel);
  if (rc == TURBO_OK)
    rc = turbo_flow_resolved_config_profile_adapter(runtime->resolved, config->profile, "output",
                                                    &output_name);
  if (rc != TURBO_OK) {
    failure_operation = "resolve profile";
    goto fail;
  }
  rc = flowie_worker_resolve_queue_channel(runtime->resolved, accept_sink_name, &queue_channel,
                                           &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "resolve Queue channel";
    goto fail;
  }
  rc = flowie_worker_resolve_session_store(runtime->resolved, endpoint_name, &session_store_channel,
                                           &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "resolve session store";
    goto fail;
  }
  rc = turbo_flow_queue_create_resolved(runtime->resolved, queue_channel, &runtime->queue,
                                        &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "create Queue";
    goto fail;
  }
  if (session_store_channel) {
    rc = flowie_worker_create_session_store(runtime->resolved, session_store_channel,
                                            &runtime->session_store,
                                            &runtime->session_store_provider, &config_error);
    if (rc != TURBO_OK) {
      failure_operation = "create session store";
      goto fail;
    }
  }
  rc = turbo_flow_rule_processor_create_resolved(
      runtime->resolved, rule_set_channel, flowie_mqtt_rule_schema(),
      flowie_mqtt_rule_facts_provider, NULL, &runtime->rule_processor, &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "create MQTT RuleSet";
    goto fail;
  }
  runtime->flow = turbo_flow_create();
  if (!runtime->flow) {
    rc = TURBO_ENOMEM;
    failure_operation = "create flow";
    goto fail;
  }
  rc = turbo_flow_configure_async_ingress(runtime->flow, &ingress_config);
  if (rc != TURBO_OK) {
    failure_operation = "configure runtime ingress";
    goto fail;
  }
  if (session_store_channel) {
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    persistence.store_channel = session_store_channel;
    persistence.store = &runtime->session_store;
    bindings.persistence = &persistence;
    rc = flowie_register_resolved_bound_endpoint(runtime->flow, endpoint_name, runtime->resolved,
                                                 &bindings, &config_error);
  } else {
    rc = flowie_register_resolved_endpoint(runtime->flow, endpoint_name, runtime->resolved,
                                           &config_error);
  }
  if (rc == TURBO_OK)
    rc = turbo_flow_queue_register_resolved_adapter(
        runtime->flow, accept_sink_name, runtime->resolved, runtime->queue, &config_error);
  if (rc == TURBO_OK)
    rc = turbo_flow_queue_register_resolved_adapter(
        runtime->flow, accepted_source_name, runtime->resolved, runtime->queue, &config_error);
  if (rc == TURBO_OK)
    rc = turbo_flow_rule_register_data_operation(runtime->flow, rule_set_channel,
                                                 runtime->rule_processor);
  if (rc == TURBO_OK)
    rc = turbo_flow_coronet_register_socket_resolved_adapter(runtime->flow, runtime->resolved,
                                                             output_name);
  if (rc != TURBO_OK) {
    failure_operation = "register primitives";
    goto fail;
  }
  rc = turbo_flow_parse_string(runtime->flow, runtime->graph.base, runtime->graph.len);
  if (rc == TURBO_OK) rc = turbo_flow_compile(runtime->flow);
  if (rc != TURBO_OK) {
    failure_operation = "compile graph";
    flow_error = turbo_flow_last_error(runtime->flow);
    goto fail;
  }
  *out = runtime;
  return TURBO_OK;

fail:
  flowie_worker_error_set(error, failure_operation, rc, &config_error, flow_error);
  (void)flowie_worker_runtime_destroy(runtime, NULL);
  return rc;
}

int flowie_worker_runtime_start(flowie_worker_runtime_t *runtime, flowie_worker_error_t *error) {
  int rc;
  flowie_worker_error_reset(error);
  if (!runtime || !runtime->flow) {
    flowie_worker_error_set(error, "start flow", TURBO_EINVAL, NULL, NULL);
    return TURBO_EINVAL;
  }
  if (runtime->started) {
    flowie_worker_error_set(error, "start flow", TURBO_EALREADY, NULL, NULL);
    return TURBO_EALREADY;
  }
  rc = turbo_flow_start(runtime->flow);
  if (rc != TURBO_OK) {
    flowie_worker_error_set(error, "start flow", rc, NULL, turbo_flow_last_error(runtime->flow));
    return rc;
  }
  runtime->started = 1;
  return TURBO_OK;
}

int flowie_worker_runtime_stop(flowie_worker_runtime_t *runtime, flowie_worker_error_t *error) {
  int rc;
  flowie_worker_error_reset(error);
  if (!runtime || !runtime->flow) {
    flowie_worker_error_set(error, "stop flow", TURBO_EINVAL, NULL, NULL);
    return TURBO_EINVAL;
  }
  if (!runtime->started) return TURBO_OK;
  rc = turbo_flow_stop(runtime->flow);
  if (rc != TURBO_OK) {
    flowie_worker_error_set(error, "stop flow", rc, NULL, turbo_flow_last_error(runtime->flow));
    return rc;
  }
  runtime->started = 0;
  return TURBO_OK;
}

int flowie_worker_runtime_destroy(flowie_worker_runtime_t *runtime, flowie_worker_error_t *error) {
  int result = TURBO_OK;
  int rc;
  flowie_worker_error_reset(error);
  if (!runtime) return TURBO_OK;
  if (runtime->started) {
    rc = flowie_worker_runtime_stop(runtime, error);
    if (rc != TURBO_OK) result = rc;
  }
  turbo_flow_destroy(runtime->flow);
  turbo_flow_rule_processor_destroy(runtime->rule_processor);
  flowie_worker_destroy_session_store(&runtime->session_store, runtime->session_store_provider);
  if (runtime->queue) {
    rc = turbo_flow_queue_destroy(runtime->queue);
    if (rc != TURBO_OK && result == TURBO_OK) {
      result = rc;
      flowie_worker_error_set(error, "destroy Queue", rc, NULL, NULL);
    }
  }
  turbo_flow_resolved_config_destroy(runtime->resolved);
  turbo_fs_buf_free(&runtime->graph);
  turbo_fs_buf_free(&runtime->yaml);
  free(runtime);
  return result;
}
