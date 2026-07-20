#include "flowie_control_runtime_internal.h"

#include "flowie_control_auth_iris_adapter_internal.h"
#include "flowie_control_auth_iris_endpoint_internal.h"
#include "flowie_control_dashboard_internal.h"
#include "flowie_control_management_rpc_internal.h"
#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "iris/iris_app.h"
#include "iris/middleware.h"
#include "iris/rpc_server.h"
#include "monocypher.h"
#include "platform.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_CSRF_KEY_SIZE 32u
#define FLOWIE_CONTROL_AUTH_ENDPOINT_BODY_MAX 8192u

struct flowie_control_runtime_s {
  flowie_control_config_t config;
  uint8_t csrf_key[FLOWIE_CONTROL_CSRF_KEY_SIZE];
  flowie_control_store_t *store;
  flowie_control_management_service_t *management_service;
  rpc_context_t *rpc_context;
  iris_app_t *app;
  flowie_control_management_rpc_server_t *management_rpc;
  flowie_control_dashboard_t *dashboard;
  flowie_control_auth_service_t *auth_service;
  flowie_control_auth_iris_adapter_t *auth_adapter;
  flowie_control_auth_iris_endpoint_t *auth_endpoint;
};

static uint64_t flowie_control_runtime_clock(void *ctx) {
  (void)ctx;
  return turbo_realtime_ms() / 1000u;
}

static int flowie_control_runtime_routes_valid(const flowie_control_config_t *config) {
  static const char *const dashboard_paths[] = {
      FLOWIE_CONTROL_DASHBOARD_PATH, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH,
      FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, FLOWIE_CONTROL_DASHBOARD_CSS_PATH,
      FLOWIE_CONTROL_DASHBOARD_HTMX_PATH};
  if (!config || !config->management.rpc_path[0]) return 0;
  if (config->auth.enabled &&
      strcmp(config->management.rpc_path, FLOWIE_CONTROL_AUTH_HTTP_PATH) == 0)
    return 0;
  if (config->dashboard_enabled) {
    for (size_t index = 0u; index < sizeof(dashboard_paths) / sizeof(dashboard_paths[0]); ++index) {
      if (strcmp(config->management.rpc_path, dashboard_paths[index]) == 0) return 0;
    }
  }
  return 1;
}

static int flowie_control_runtime_env_secret(const char *reference, const char **value_out) {
  static const char prefix[] = "env://";
  const char *value;
  const char *name;
  if (value_out) *value_out = NULL;
  if (!reference || !value_out) return TURBO_EINVAL;
  if (!reference[0]) return TURBO_OK;
  if (strncmp(reference, prefix, sizeof(prefix) - 1u) != 0 ||
      !(name = reference + sizeof(prefix) - 1u)[0])
    return TURBO_EINVAL;
  value = getenv(name);
  if (!value || !value[0]) return TURBO_ENOENT;
  *value_out = value;
  return TURBO_OK;
}

static int flowie_control_runtime_tls_config(const flowie_control_config_t *config,
                                             turbo_tls_server_config_t *tls_out) {
  const char *password = NULL;
  int rc;
  if (!config || config->size < sizeof(*config) || config->version != FLOWIE_CONTROL_CONFIG_VERSION ||
      !tls_out)
    return TURBO_EINVAL;
  rc = flowie_control_runtime_env_secret(config->listener.tls.key_password_ref, &password);
  if (rc != TURBO_OK) return rc;
  *tls_out = (turbo_tls_server_config_t){sizeof(*tls_out),
                                        config->listener.tls.cert_file,
                                        config->listener.tls.key_file,
                                        password,
                                        config->listener.tls.client_ca_file,
                                        NULL,
                                        TURBO_TLS_CLIENT_AUTH_REQUIRED};
  return TURBO_OK;
}

int flowie_control_runtime_validate(const flowie_control_config_t *config) {
  turbo_tls_server_config_t tls = {0};
  coro_context_t *context = NULL;
  coro_socket_t *socket = NULL;
  int rc;
  if (!flowie_control_runtime_routes_valid(config)) return TURBO_EINVAL;
  rc = flowie_control_runtime_tls_config(config, &tls);
  if (rc != TURBO_OK) return rc;
  context = coro_context_create(NULL);
  if (!context) return TURBO_ENOMEM;
  socket = coro_socket_create(context, CORO_SOCKET_TLS);
  if (!socket) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = coro_socket_set_tls_server_config(socket, &tls);

done:
  if (socket) coro_socket_destroy(socket);
  coro_context_destroy(context);
  return rc;
}

static uint32_t flowie_control_runtime_permissions(
    const flowie_control_effective_roles_view_t *roles) {
  uint32_t permissions = 0u;
  if (!roles || roles->size < sizeof(*roles)) return 0u;
  for (uint32_t index = 0u; index < roles->role_count; ++index) {
    const char *role = roles->roles[index];
    if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_POLICY_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
  }
  return permissions;
}

int flowie_control_management_identity_resolve(
    flowie_control_store_t *store, const flowie_control_config_admin_binding_t *bindings,
    size_t binding_count, const char *fingerprint,
    flowie_control_management_caller_t *caller_out) {
  flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
  flowie_control_effective_roles_view_t roles = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  const flowie_control_config_admin_binding_t *binding = NULL;
  int rc;
  if (caller_out && caller_out->size >= sizeof(*caller_out)) *caller_out = caller;
  if (!store || !bindings || binding_count == 0u ||
      binding_count > FLOWIE_CONTROL_CONFIG_MAX_ADMIN_BINDINGS || !fingerprint || !caller_out ||
      caller_out->size < sizeof(*caller_out))
    return TURBO_EINVAL;
  for (size_t index = 0u; index < binding_count; ++index) {
    const flowie_control_config_admin_binding_t *candidate = &bindings[index];
    if (strcmp(candidate->peer_certificate_sha256, fingerprint) == 0) {
      binding = candidate;
      break;
    }
  }
  if (!binding) return TURBO_EPERM;
  rc = flowie_control_store_user_get(store, binding->root_group_id, binding->principal_id, &user);
  if (rc != TURBO_OK || !user.enabled) return TURBO_EPERM;
  rc = flowie_control_store_effective_roles(store, binding->root_group_id, binding->principal_id,
                                            &roles);
  if (rc != TURBO_OK) return rc == TURBO_EINVAL ? rc : TURBO_EPERM;
  caller.permissions = flowie_control_runtime_permissions(&roles);
  if (caller.permissions == 0u) return TURBO_EPERM;
  caller.root_group_id = binding->root_group_id;
  caller.actor = binding->principal_id;
  *caller_out = caller;
  return TURBO_OK;
}

int flowie_control_runtime_resolve_management_fingerprint(
    flowie_control_runtime_t *runtime, const char *fingerprint,
    flowie_control_management_caller_t *caller_out) {
  if (!runtime) return TURBO_EINVAL;
  return flowie_control_management_identity_resolve(
      runtime->store, runtime->config.management.admin_bindings,
      runtime->config.management.admin_binding_count, fingerprint, caller_out);
}

static int flowie_control_runtime_fingerprint(const Req *request,
                                              char output[CORO_TLS_PEER_CERT_SHA256_CAPACITY]) {
  if (!request || !output) return TURBO_EINVAL;
  return req_get_verified_tls_peer_certificate_sha256(request, output) == TURBO_OK ? TURBO_OK
                                                                                  : TURBO_EPERM;
}

static int flowie_control_runtime_mtls_middleware(Req *request, Res *response, Chain *chain) {
  char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  int rc;
  if (!request || !response || !chain || !request->security) return 1;
  rc = flowie_control_runtime_fingerprint(request, fingerprint);
  crypto_wipe(fingerprint, sizeof(fingerprint));
  if (rc != TURBO_OK) {
    send_json(response, 401, "{\"error\":\"Authentication required\"}");
    return 1;
  }
  request->security->authenticated = true;
  return next(chain, request, response);
}

static int flowie_control_runtime_resolve_caller(
    void *ctx, const Req *request, flowie_control_management_caller_t *caller_out) {
  char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  int rc = flowie_control_runtime_fingerprint(request, fingerprint);
  if (rc == TURBO_OK)
    rc = flowie_control_runtime_resolve_management_fingerprint(
        (flowie_control_runtime_t *)ctx, fingerprint, caller_out);
  crypto_wipe(fingerprint, sizeof(fingerprint));
  return rc;
}

static int flowie_control_runtime_resolve_session(
    void *ctx, const Req *request, flowie_control_management_caller_t *caller_out,
    char csrf_token_out[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]) {
  static const char hex[] = "0123456789abcdef";
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY] = {0};
  uint8_t digest[FLOWIE_CONTROL_CSRF_KEY_SIZE] = {0};
  int rc;
  if (csrf_token_out) csrf_token_out[0] = '\0';
  if (!runtime || !csrf_token_out) return TURBO_EINVAL;
  rc = flowie_control_runtime_fingerprint(request, fingerprint);
  if (rc == TURBO_OK)
    rc = flowie_control_runtime_resolve_management_fingerprint(runtime, fingerprint, caller_out);
  if (rc == TURBO_OK) {
    crypto_blake2b_keyed(digest, sizeof(digest), runtime->csrf_key, sizeof(runtime->csrf_key),
                         (const uint8_t *)fingerprint, strlen(fingerprint));
    for (size_t index = 0u; index < sizeof(digest); ++index) {
      csrf_token_out[index * 2u] = hex[digest[index] >> 4u];
      csrf_token_out[index * 2u + 1u] = hex[digest[index] & 0x0fu];
    }
    csrf_token_out[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE] = '\0';
  }
  crypto_wipe(digest, sizeof(digest));
  crypto_wipe(fingerprint, sizeof(fingerprint));
  return rc;
}

static int flowie_control_runtime_policy_version(void *ctx, const char *root_group_id,
                                                 uint64_t *policy_version_out) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  int rc;
  if (policy_version_out) *policy_version_out = 0u;
  if (!runtime || !runtime->store || !root_group_id || !policy_version_out) return TURBO_EINVAL;
  rc = flowie_control_store_policy_status(runtime->store, root_group_id, &status);
  if (rc == TURBO_OK) *policy_version_out = status.policy_version;
  return rc;
}

static int flowie_control_runtime_secret_acquire(
    void *ctx, const char *reference, turbo_flow_security_secret_lease_t *lease_out) {
  const char *value = NULL;
  int rc;
  (void)ctx;
  if (!lease_out || lease_out->size < sizeof(*lease_out)) return TURBO_EINVAL;
  rc = flowie_control_runtime_env_secret(reference, &value);
  if (rc != TURBO_OK) return rc;
  *lease_out = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  lease_out->bytes = (const uint8_t *)value;
  lease_out->byte_count = strlen(value);
  lease_out->provider_lease = (void *)value;
  return TURBO_OK;
}

static void flowie_control_runtime_secret_release(void *ctx,
                                                  turbo_flow_security_secret_lease_t *lease) {
  (void)ctx;
  (void)lease;
}

static int flowie_control_runtime_create_auth(flowie_control_runtime_t *runtime) {
  flowie_control_auth_root_binding_t bindings[FLOWIE_CONTROL_AUTH_MAX_BINDINGS];
  flowie_control_auth_service_config_t service_config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_auth_iris_adapter_config_t adapter_config =
      FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
  flowie_control_auth_iris_endpoint_config_t endpoint_config =
      FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT;
  int rc;
  if (!runtime->config.auth.enabled) return TURBO_OK;
  memset(bindings, 0, sizeof(bindings));
  for (size_t index = 0u; index < runtime->config.auth.binding_count; ++index) {
    flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
    bindings[index] = (flowie_control_auth_root_binding_t)FLOWIE_CONTROL_AUTH_ROOT_BINDING_INIT;
    bindings[index].listener_id = runtime->config.auth.listener_id;
    bindings[index].peer_certificate_sha256 =
        runtime->config.auth.bindings[index].peer_certificate_sha256;
    bindings[index].root_group_id = runtime->config.auth.bindings[index].root_group_id;
    rc = flowie_control_store_policy_status(runtime->store, bindings[index].root_group_id, &status);
    if (rc != TURBO_OK) return rc;
  }
  service_config.store = runtime->store;
  service_config.bindings = bindings;
  service_config.binding_count = runtime->config.auth.binding_count;
  service_config.method = runtime->config.auth.method;
  service_config.principal_ttl_seconds = runtime->config.auth.principal_ttl_seconds;
  service_config.credential_cache.capacity = runtime->config.auth.credential_cache_capacity;
  service_config.credential_cache.ttl_ms =
      runtime->config.auth.credential_cache_ttl_seconds * 1000u;
  service_config.principal_cache.capacity = runtime->config.auth.credential_cache_capacity;
  service_config.principal_cache.ttl_ms =
      runtime->config.auth.credential_cache_ttl_seconds * 1000u;
  service_config.policy_version.ctx = runtime;
  service_config.policy_version.current = flowie_control_runtime_policy_version;
  rc = flowie_control_auth_service_create(&service_config, &runtime->auth_service);
  if (rc != TURBO_OK) return rc;
  adapter_config.service = runtime->auth_service;
  adapter_config.listener_id = runtime->config.auth.listener_id;
  rc = flowie_control_auth_iris_adapter_create(&adapter_config, &runtime->auth_adapter);
  if (rc != TURBO_OK) return rc;
  endpoint_config.adapter = runtime->auth_adapter;
  endpoint_config.service_token_ref = runtime->config.auth.service_token_ref;
  endpoint_config.key_provider.ctx = runtime;
  endpoint_config.key_provider.acquire = flowie_control_runtime_secret_acquire;
  endpoint_config.key_provider.release = flowie_control_runtime_secret_release;
  endpoint_config.max_request_body_size = FLOWIE_CONTROL_AUTH_ENDPOINT_BODY_MAX;
  rc = flowie_control_auth_iris_endpoint_create(&endpoint_config, &runtime->auth_endpoint);
  if (rc != TURBO_OK) return rc;
  return flowie_control_auth_iris_endpoint_register(runtime->auth_endpoint, runtime->app);
}

int flowie_control_runtime_create(const flowie_control_config_t *config,
                                  flowie_control_runtime_t **out) {
  flowie_control_runtime_t *runtime = NULL;
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_management_service_config_t management_config =
      FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_management_rpc_server_config_t rpc_server_config =
      FLOWIE_CONTROL_MANAGEMENT_RPC_SERVER_CONFIG_INIT;
  flowie_control_dashboard_config_t dashboard_config = FLOWIE_CONTROL_DASHBOARD_CONFIG_INIT;
  iris_security_limits_t limits;
  rpc_config_t rpc_config = RPC_DEFAULT_CONFIG();
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || config->version != FLOWIE_CONTROL_CONFIG_VERSION ||
      !out)
    return TURBO_EINVAL;
  rc = flowie_control_runtime_validate(config);
  if (rc != TURBO_OK) return rc;
  runtime = (flowie_control_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  runtime->config = *config;
  if (turbo_secure_random(runtime->csrf_key, sizeof(runtime->csrf_key)) != TURBO_OK) {
    rc = TURBO_EIO;
    goto fail;
  }
  store_config.database_path = runtime->config.sqlite_path;
  store_config.busy_timeout_ms = runtime->config.sqlite_busy_timeout_ms;
  rc = flowie_control_store_open(&store_config, &runtime->store);
  if (rc != TURBO_OK) goto fail;
  management_config.store = runtime->store;
  rc = flowie_control_management_service_create(&management_config, &runtime->management_service);
  if (rc != TURBO_OK) goto fail;
  runtime->app = iris_app_create();
  if (!runtime->app) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  iris_app_hook(runtime->app, flowie_control_runtime_mtls_middleware);
  limits = (iris_security_limits_t){runtime->config.listener.limits.max_header_name_length,
                                    runtime->config.listener.limits.max_header_value_length,
                                    runtime->config.listener.limits.max_url_length,
                                    runtime->config.listener.limits.max_cookie_name_length,
                                    runtime->config.listener.limits.max_cookie_value_length,
                                    runtime->config.listener.limits.max_json_depth,
                                    runtime->config.listener.limits.max_log_message_length,
                                    runtime->config.listener.limits.max_request_body_size,
                                    runtime->config.listener.limits.max_headers_count};
  iris_app_set_security_limits(runtime->app, &limits);
  rpc_config.endpoint = runtime->config.management.rpc_path;
  rpc_config.enable_introspection = 0;
  rpc_config.enable_batch = 0;
  rpc_config.max_batch_size = 1;
  rpc_config.max_request_size = runtime->config.management.rpc_max_request_size;
  runtime->rpc_context = rpc_init(&rpc_config);
  if (!runtime->rpc_context) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rpc_server_config.service = runtime->management_service;
  rpc_server_config.rpc_context = runtime->rpc_context;
  rpc_server_config.resolve_caller = flowie_control_runtime_resolve_caller;
  rpc_server_config.resolve_caller_ctx = runtime;
  rpc_server_config.clock = flowie_control_runtime_clock;
  rc = flowie_control_management_rpc_server_create(&rpc_server_config, &runtime->management_rpc);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_management_rpc_server_bind(runtime->management_rpc, runtime->app);
  if (rc != TURBO_OK) goto fail;
  for (size_t index = 0u; index < runtime->config.management.admin_binding_count; ++index) {
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    rc = flowie_control_runtime_resolve_management_fingerprint(
        runtime, runtime->config.management.admin_bindings[index].peer_certificate_sha256, &caller);
    if (rc != TURBO_OK) goto fail;
  }
  if (runtime->config.dashboard_enabled) {
    dashboard_config.service = runtime->management_service;
    dashboard_config.resolve_session = flowie_control_runtime_resolve_session;
    dashboard_config.resolve_session_ctx = runtime;
    rc = flowie_control_dashboard_create(&dashboard_config, &runtime->dashboard);
    if (rc != TURBO_OK) goto fail;
    rc = flowie_control_dashboard_bind(runtime->dashboard, runtime->app);
    if (rc != TURBO_OK) goto fail;
  }
  rc = flowie_control_runtime_create_auth(runtime);
  if (rc != TURBO_OK) goto fail;
  *out = runtime;
  return TURBO_OK;

fail:
  flowie_control_runtime_destroy(runtime);
  return rc;
}

int flowie_control_runtime_run(flowie_control_runtime_t *runtime) {
  turbo_tls_server_config_t tls = {0};
  int rc;
  if (!runtime || !runtime->app) return TURBO_EINVAL;
  rc = flowie_control_runtime_tls_config(&runtime->config, &tls);
  if (rc != TURBO_OK) return rc;
  return iris_app_listen_tls_on(runtime->app, runtime->config.listener.host,
                                runtime->config.listener.port, &tls) == 0
             ? TURBO_OK
             : TURBO_EIO;
}

void flowie_control_runtime_destroy(flowie_control_runtime_t *runtime) {
  if (!runtime) return;
  flowie_control_auth_iris_endpoint_destroy(runtime->auth_endpoint);
  runtime->auth_endpoint = NULL;
  flowie_control_auth_iris_adapter_destroy(runtime->auth_adapter);
  runtime->auth_adapter = NULL;
  flowie_control_auth_service_destroy(runtime->auth_service);
  runtime->auth_service = NULL;
  flowie_control_dashboard_destroy(runtime->dashboard);
  runtime->dashboard = NULL;
  flowie_control_management_rpc_server_destroy(runtime->management_rpc);
  runtime->management_rpc = NULL;
  iris_app_destroy(runtime->app);
  runtime->app = NULL;
  rpc_destroy(runtime->rpc_context);
  runtime->rpc_context = NULL;
  flowie_control_management_service_destroy(runtime->management_service);
  runtime->management_service = NULL;
  flowie_control_store_destroy(runtime->store);
  runtime->store = NULL;
  crypto_wipe(runtime, sizeof(*runtime));
  free(runtime);
}
