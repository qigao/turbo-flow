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
  turbo_flow_security_realm_t *security_realm;
  turbo_flow_security_auth_provider_owner_t auth_provider;
  turbo_flow_security_policy_provider_owner_t policy_provider;
  turbo_flow_rule_processor_t *rule_processor;
  turbo_flow_t *flow;
  int started;
};

typedef struct flowie_worker_provider_context_s {
  flowie_worker_runtime_t *runtime;
  const char *endpoint_name;
  const char *accept_sink_name;
  const char *accepted_source_name;
  const char *rule_set_channel;
  const char *output_name;
  const char *queue_channel;
  const char *session_store_channel;
  const char *security_realm_channel;
  const char *security_auth_method;
} flowie_worker_provider_context_t;

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
                   "Queue adapter must reference a configured Queue channel");
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

static int flowie_worker_resolve_security(const turbo_flow_resolved_config_t *resolved,
                                          const char *profile, const char *endpoint_name,
                                          const char **realm_channel, const char **auth_method,
                                          const char **provider_channel,
                                          turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  int realm_rc;
  int method_rc;
  int rc;
  if (realm_channel) *realm_channel = NULL;
  if (auth_method) *auth_method = NULL;
  if (provider_channel) *provider_channel = NULL;
  if (!resolved || !profile || !profile[0] || !endpoint_name || !endpoint_name[0] ||
      !realm_channel || !auth_method || !provider_channel || !error)
    return TURBO_EINVAL;
  rc = turbo_flow_resolved_config_adapter(resolved, endpoint_name, &view);
  if (rc == TURBO_OK && strcmp(view.kind, "flowie_endpoint") != 0) rc = TURBO_EINVAL;
  if (rc != TURBO_OK) goto invalid;
  realm_rc = turbo_flow_resolved_adapter_get_string(&view, "security_realm", realm_channel);
  method_rc = turbo_flow_resolved_adapter_get_string(&view, "auth_method", auth_method);
  if (realm_rc == TURBO_ENOENT && method_rc == TURBO_ENOENT) return TURBO_OK;
  if (realm_rc != TURBO_OK || method_rc != TURBO_OK || !*realm_channel || !(*realm_channel)[0] ||
      !*auth_method || !(*auth_method)[0]) {
    rc = realm_rc != TURBO_OK && realm_rc != TURBO_ENOENT ? realm_rc
         : method_rc != TURBO_OK                          ? method_rc
                                                          : TURBO_EINVAL;
    goto invalid;
  }
  rc = turbo_flow_resolved_config_profile_channel(resolved, profile, "auth_provider",
                                                  provider_channel);
  if (rc != TURBO_OK || !*provider_channel || !(*provider_channel)[0]) goto invalid;
  return TURBO_OK;

invalid:
  *realm_channel = NULL;
  *auth_method = NULL;
  *provider_channel = NULL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  error->status = rc == TURBO_OK ? TURBO_EINVAL : rc;
  (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config", endpoint_name);
  (void)snprintf(
      error->message, sizeof(error->message),
      "secure endpoints require security_realm, auth_method, and profiles.%s.auth_provider",
      profile);
  return error->status;
}

static int flowie_worker_env_secret_acquire(void *ctx, const char *reference,
                                            turbo_flow_security_secret_lease_t *lease_out) {
  static const char prefix[] = "env://";
  const char *value;
  const char *name;
  (void)ctx;
  if (!reference || strncmp(reference, prefix, sizeof(prefix) - 1u) != 0 ||
      !(name = reference + sizeof(prefix) - 1u)[0] || !lease_out ||
      lease_out->size < sizeof(*lease_out))
    return TURBO_EINVAL;
  value = getenv(name);
  if (!value || !value[0]) return TURBO_ENOENT;
  *lease_out = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  lease_out->bytes = (const uint8_t *)value;
  lease_out->byte_count = strlen(value);
  lease_out->provider_lease = (void *)value;
  return TURBO_OK;
}

static void flowie_worker_env_secret_release(void *ctx, turbo_flow_security_secret_lease_t *lease) {
  (void)ctx;
  (void)lease;
}

static int flowie_worker_create_auth_provider(
    const flowie_worker_runtime_config_t *worker_config,
    const turbo_flow_resolved_config_t *resolved, const char *channel,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_auth_provider_owner_t *owner, turbo_flow_config_error_t *error) {
  if (!worker_config) return TURBO_EINVAL;
  return turbo_flow_security_auth_provider_owner_create_registered(
      worker_config->auth_provider_factories, worker_config->auth_provider_factory_count, resolved,
      channel, key_provider, owner, error);
}

static int flowie_worker_create_policy_provider(
    const flowie_worker_runtime_config_t *worker_config,
    const turbo_flow_resolved_config_t *resolved, const char *channel,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_policy_provider_owner_t *owner, turbo_flow_config_error_t *error) {
  if (!worker_config) return TURBO_EINVAL;
  return turbo_flow_security_policy_provider_owner_create_registered(
      worker_config->policy_provider_factories, worker_config->policy_provider_factory_count,
      resolved, channel, key_provider, owner, error);
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

static int flowie_worker_register_rule_resource(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *resource_name,
                                                turbo_flow_config_error_t *error) {
  flowie_worker_provider_context_t *provider = (flowie_worker_provider_context_t *)ctx;
  int rc;
  if (!provider || !provider->runtime || !provider->rule_set_channel ||
      strcmp(resource_name, provider->rule_set_channel) != 0 || provider->runtime->rule_processor) {
    return TURBO_EINVAL;
  }
  rc = turbo_flow_rule_processor_create_resolved(resolved, resource_name, flowie_mqtt_rule_schema(),
                                                 flowie_mqtt_rule_facts_provider, NULL,
                                                 &provider->runtime->rule_processor, error);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_rule_register_data_operation(flow, resource_name,
                                               provider->runtime->rule_processor);
  if (rc != TURBO_OK) {
    turbo_flow_rule_processor_destroy(provider->runtime->rule_processor);
    provider->runtime->rule_processor = NULL;
  }
  return rc;
}

static int flowie_worker_register_endpoint_adapter(void *ctx, turbo_flow_t *flow,
                                                   const turbo_flow_resolved_config_t *resolved,
                                                   const char *adapter_name,
                                                   turbo_flow_config_error_t *error) {
  flowie_worker_provider_context_t *provider = (flowie_worker_provider_context_t *)ctx;
  if (!provider || !provider->runtime || !provider->endpoint_name ||
      strcmp(adapter_name, provider->endpoint_name) != 0) {
    return TURBO_EINVAL;
  }
  if (provider->session_store_channel || provider->security_realm_channel) {
    flowie_endpoint_persistence_binding_t persistence = FLOWIE_ENDPOINT_PERSISTENCE_BINDING_INIT;
    flowie_endpoint_security_binding_t security = FLOWIE_ENDPOINT_SECURITY_BINDING_INIT;
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    if (provider->session_store_channel) {
      persistence.store_channel = provider->session_store_channel;
      persistence.store = &provider->runtime->session_store;
      bindings.persistence = &persistence;
    }
    if (provider->security_realm_channel) {
      security.realm_channel = provider->security_realm_channel;
      security.auth_method = provider->security_auth_method;
      security.auth_provider = provider->runtime->auth_provider.provider;
      security.enhanced_auth_provider = provider->runtime->auth_provider.enhanced_provider;
      security.realm = provider->runtime->security_realm;
      bindings.security = &security;
    }
    return flowie_register_resolved_bound_endpoint(flow, adapter_name, resolved, &bindings, error);
  }
  return flowie_register_resolved_endpoint(flow, adapter_name, resolved, error);
}

static int flowie_worker_register_queue_adapter(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_resolved_config_t *resolved,
                                                const char *adapter_name,
                                                turbo_flow_config_error_t *error) {
  flowie_worker_provider_context_t *provider = (flowie_worker_provider_context_t *)ctx;
  const char *channel = NULL;
  int rc;
  if (!provider || !provider->runtime || !provider->runtime->queue ||
      (!provider->accept_sink_name || strcmp(adapter_name, provider->accept_sink_name) != 0) &&
          (!provider->accepted_source_name ||
           strcmp(adapter_name, provider->accepted_source_name) != 0)) {
    return TURBO_EINVAL;
  }
  rc = flowie_worker_resolve_queue_channel(resolved, adapter_name, &channel, error);
  if (rc != TURBO_OK) return rc;
  if (!provider->queue_channel || strcmp(channel, provider->queue_channel) != 0) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = TURBO_EINVAL;
    (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.channel", adapter_name);
    (void)snprintf(error->message, sizeof(error->message),
                   "Flowie Queue source and sink must reference the same channel");
    return TURBO_EINVAL;
  }
  return turbo_flow_queue_register_resolved_adapter(flow, adapter_name, resolved,
                                                    provider->runtime->queue, error);
}

static int flowie_worker_register_socket_adapter(void *ctx, turbo_flow_t *flow,
                                                 const turbo_flow_resolved_config_t *resolved,
                                                 const char *adapter_name,
                                                 turbo_flow_config_error_t *error) {
  flowie_worker_provider_context_t *provider = (flowie_worker_provider_context_t *)ctx;
  (void)error;
  if (!provider || !provider->output_name || strcmp(adapter_name, provider->output_name) != 0)
    return TURBO_EINVAL;
  return turbo_flow_coronet_register_socket_resolved_adapter(flow, resolved, adapter_name);
}

int flowie_worker_runtime_create(const flowie_worker_runtime_config_t *config,
                                 flowie_worker_runtime_t **out, flowie_worker_error_t *error) {
  const char *endpoint_name = NULL;
  const char *accept_sink_name = NULL;
  const char *accepted_source_name = NULL;
  const char *rule_set_channel = NULL;
  const char *output_name = NULL;
  const char *queue_channel = NULL;
  const char *session_store_channel = NULL;
  const char *security_realm_channel = NULL;
  const char *security_auth_method = NULL;
  const char *auth_provider_channel = NULL;
  const char *failure_operation = NULL;
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_async_ingress_config_t ingress_config = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  flowie_worker_provider_context_t provider_context = {0};
  turbo_flow_product_adapter_provider_t adapter_providers[3] = {
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT, TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT,
      TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT};
  turbo_flow_product_resource_provider_t resource_provider =
      TURBO_FLOW_PRODUCT_RESOURCE_PROVIDER_INIT;
  turbo_flow_product_provider_registry_t provider_registry =
      TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
  turbo_flow_security_key_provider_t key_provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
  const turbo_flow_error_t *flow_error = NULL;
  flowie_worker_runtime_t *runtime;
  int rc;

  if (out) *out = NULL;
  flowie_worker_error_reset(error);
  if (!config || config->size != sizeof(*config) || !out || !config->profile ||
      !config->profile[0] || !config->config_path || !config->config_path[0] ||
      !config->graph_path || !config->graph_path[0] ||
      (config->auth_provider_factory_count > 0u && !config->auth_provider_factories) ||
      (config->policy_provider_factory_count > 0u && !config->policy_provider_factories)) {
    flowie_worker_error_set(error, "validate worker configuration", TURBO_EINVAL, NULL, NULL);
    return TURBO_EINVAL;
  }
  runtime = (flowie_worker_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) {
    flowie_worker_error_set(error, "create worker", TURBO_ENOMEM, NULL, NULL);
    return TURBO_ENOMEM;
  }
  runtime->session_store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  runtime->auth_provider =
      (turbo_flow_security_auth_provider_owner_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
  runtime->policy_provider =
      (turbo_flow_security_policy_provider_owner_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
  key_provider.acquire = flowie_worker_env_secret_acquire;
  key_provider.release = flowie_worker_env_secret_release;
  provider_context.runtime = runtime;
  adapter_providers[0].kind = "flowie_endpoint";
  adapter_providers[0].register_adapter = flowie_worker_register_endpoint_adapter;
  adapter_providers[0].ctx = &provider_context;
  adapter_providers[1].kind = "queue";
  adapter_providers[1].register_adapter = flowie_worker_register_queue_adapter;
  adapter_providers[1].ctx = &provider_context;
  adapter_providers[2].kind = "socket";
  adapter_providers[2].register_adapter = flowie_worker_register_socket_adapter;
  adapter_providers[2].ctx = &provider_context;
  resource_provider.kind = "rule_set";
  resource_provider.register_resource = flowie_worker_register_rule_resource;
  resource_provider.ctx = &provider_context;
  provider_registry.adapter_providers = adapter_providers;
  provider_registry.adapter_provider_count =
      sizeof(adapter_providers) / sizeof(adapter_providers[0]);
  provider_registry.resource_providers = &resource_provider;
  provider_registry.resource_provider_count = 1u;

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
  rc = turbo_flow_product_preflight(runtime->resolved, &provider_registry, &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "preflight product providers";
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
  provider_context.endpoint_name = endpoint_name;
  provider_context.accept_sink_name = accept_sink_name;
  provider_context.accepted_source_name = accepted_source_name;
  provider_context.rule_set_channel = rule_set_channel;
  provider_context.output_name = output_name;
  rc = flowie_worker_resolve_queue_channel(runtime->resolved, accept_sink_name, &queue_channel,
                                           &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "resolve Queue channel";
    goto fail;
  }
  provider_context.queue_channel = queue_channel;
  rc = flowie_worker_resolve_session_store(runtime->resolved, endpoint_name, &session_store_channel,
                                           &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "resolve session store";
    goto fail;
  }
  provider_context.session_store_channel = session_store_channel;
  rc = flowie_worker_resolve_security(runtime->resolved, config->profile, endpoint_name,
                                      &security_realm_channel, &security_auth_method,
                                      &auth_provider_channel, &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "resolve endpoint security";
    goto fail;
  }
  provider_context.security_realm_channel = security_realm_channel;
  provider_context.security_auth_method = security_auth_method;
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
  if (security_realm_channel) {
    const char *policy_source;
    rc = turbo_flow_security_realm_create_resolved(runtime->resolved, security_realm_channel, NULL,
                                                   &runtime->security_realm, &config_error);
    if (rc != TURBO_OK) {
      failure_operation = "create security realm";
      goto fail;
    }
    policy_source = turbo_flow_security_realm_policy_source(runtime->security_realm);
    rc = flowie_worker_create_policy_provider(config, runtime->resolved, policy_source,
                                              &key_provider, &runtime->policy_provider,
                                              &config_error);
    if (rc != TURBO_OK) {
      failure_operation = "create ACL policy provider";
      goto fail;
    }
    rc = turbo_flow_security_realm_bind_policy_provider(runtime->security_realm,
                                                        runtime->policy_provider.provider);
    if (rc != TURBO_OK) {
      failure_operation = "bind ACL policy provider";
      goto fail;
    }
    rc = flowie_worker_create_auth_provider(config, runtime->resolved, auth_provider_channel,
                                            &key_provider, &runtime->auth_provider, &config_error);
    if (rc != TURBO_OK) {
      failure_operation = "create authentication provider";
      goto fail;
    }
    if (strcmp(runtime->auth_provider.method, security_auth_method) != 0) {
      rc = TURBO_EINVAL;
      config_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      config_error.status = rc;
      (void)snprintf(config_error.path, sizeof(config_error.path),
                     "$.adapters.%s.config.auth_method", endpoint_name);
      (void)snprintf(config_error.message, sizeof(config_error.message),
                     "endpoint auth_method must match the authentication provider method");
      failure_operation = "bind authentication provider";
      goto fail;
    }
  }
  runtime->flow = turbo_flow_create();
  if (!runtime->flow) {
    rc = TURBO_ENOMEM;
    failure_operation = "create flow";
    goto fail;
  }
  if (runtime->security_realm) {
    rc = turbo_flow_security_realm_register(runtime->flow, runtime->security_realm);
    if (rc != TURBO_OK) {
      failure_operation = "register security realm";
      goto fail;
    }
  }
  rc = turbo_flow_configure_async_ingress(runtime->flow, &ingress_config);
  if (rc != TURBO_OK) {
    failure_operation = "configure runtime ingress";
    goto fail;
  }
  rc = turbo_flow_parse_string(runtime->flow, runtime->graph.base, runtime->graph.len);
  if (rc != TURBO_OK) {
    failure_operation = "parse graph";
    flow_error = turbo_flow_last_error(runtime->flow);
    goto fail;
  }
  rc = turbo_flow_product_assemble_graph(runtime->flow, runtime->resolved, &provider_registry,
                                         &config_error);
  if (rc != TURBO_OK) {
    failure_operation = "assemble product graph";
    goto fail;
  }
  rc = turbo_flow_compile(runtime->flow);
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
  turbo_flow_security_auth_provider_owner_destroy(&runtime->auth_provider);
  turbo_flow_security_realm_destroy(runtime->security_realm);
  turbo_flow_security_policy_provider_owner_destroy(&runtime->policy_provider);
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
