#include "flowie_control_runtime_internal.h"
#include "flowie_control_http_request_internal.h"

#include "platform.h"
#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_object_pool.h"
#include "CoroNet/turbo_coro_socket.h"
#include "flowie_control_acl_iris_endpoint_internal.h"
#include "flowie_control_auth_iris_adapter_internal.h"
#include "flowie_control_auth_iris_endpoint_internal.h"
#include "flowie_control_bootstrap_internal.h"
#include "flowie_control_dashboard_internal.h"
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
  #include "flowie_control_external_https_authenticator_internal.h"
#endif
#if defined(FLOWIE_CONTROL_HAS_PGSQL)
  #include "flowie_control_pgsql_repository_internal.h"
#endif
#include "flowie_control_management_rpc_internal.h"
#include "flowie_control_management_session_internal.h"
#include "flowie_control_service_credential_internal.h"
#include "iris/cookie.h"
#include "iris/iris_app.h"
#include "iris/middleware.h"
#include "iris/rpc_server.h"
#include "iris/server.h"
#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_AUTH_ENDPOINT_BODY_MAX 8192u
#define FLOWIE_CONTROL_RUNTIME_SESSION_CONTEXT "/.flowie/internal/session-runtime"

struct flowie_control_runtime_s {
  flowie_control_config_t config;
  flowie_control_store_t *store;
#if defined(FLOWIE_CONTROL_HAS_PGSQL)
  flowie_control_pgsql_repository_provider_t *pgsql_provider;
#endif
  const flowie_control_repository_t *repository;
  flowie_control_management_service_t *management_service;
  flowie_control_auth_service_t *management_auth_service;
  flowie_control_management_session_store_t *management_sessions;
  rpc_context_t *rpc_context;
  iris_app_t *app;
  flowie_control_management_rpc_server_t *management_rpc;
  flowie_control_dashboard_t *dashboard;
  flowie_control_auth_service_t *auth_service;
  flowie_control_service_credential_resolver_t *service_credentials;
  flowie_control_auth_iris_adapter_t *auth_adapter;
  flowie_control_auth_iris_endpoint_t *auth_endpoint;
  flowie_control_acl_iris_endpoint_t *acl_endpoint;
  coro_context_t *listener_context;
  coro_socket_t *listener;
  coro_wait_t *listener_stop_wait;
  turbo_thread_t listener_thread;
  atomic_int listener_started;
  atomic_int listener_shutdown_status;
  int listener_thread_started;
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
  flowie_control_external_https_authenticator_t *external_https_authenticator;
  flowie_control_external_subject_mapper_t *external_subject_mapper;
#endif
};

static void flowie_control_runtime_listener_shutdown(coro_t *coroutine, void *ctx) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  int rc = TURBO_OK;
  (void)coroutine;
  if (!runtime || !runtime->listener_context || !runtime->listener_stop_wait) return;
  while (atomic_load_explicit(&runtime->listener_started, memory_order_acquire)) {
    rc = coro_wait_for(runtime->listener_stop_wait, 3600000u);
    if (rc != TURBO_OK && rc != TURBO_ECANCELED) break;
  }
  if (runtime->listener) {
    rc = coro_socket_server_stop(runtime->listener);
    while (rc == TURBO_OK && !coro_socket_server_is_stopped(runtime->listener))
      coro_sleep(runtime->listener_context, 1u);
    if (rc == TURBO_OK) {
      coro_socket_destroy(runtime->listener);
      runtime->listener = NULL;
    }
  }
  atomic_store_explicit(&runtime->listener_shutdown_status, rc, memory_order_release);
  coro_context_stop(runtime->listener_context);
}

static void flowie_control_runtime_listener_thread(void *ctx) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  if (!runtime || !runtime->listener_context) return;
  (void)coro_context_run(runtime->listener_context, TURBO_RUN_DEFAULT);
}

#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
static int flowie_control_runtime_validate_external_https(const flowie_control_config_t *config);
#endif

static uint64_t flowie_control_runtime_clock(void *ctx) {
  (void)ctx;
  return turbo_realtime_ms() / 1000u;
}

static int flowie_control_runtime_external_https_stats(
    void *ctx, flowie_control_external_https_authenticator_stats_t *stats_out) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  if (!runtime || !stats_out || stats_out->size < sizeof(*stats_out)) return TURBO_EINVAL;
  *stats_out = (flowie_control_external_https_authenticator_stats_t)
      FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_STATS_INIT;
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
  if (!runtime->external_https_authenticator) return TURBO_ENOENT;
  return flowie_control_external_https_authenticator_get_stats(
      runtime->external_https_authenticator, stats_out);
#else
  return TURBO_ENOENT;
#endif
}

static int flowie_control_runtime_routes_valid(const flowie_control_config_t *config) {
  static const char *const dashboard_paths[] = {
      FLOWIE_CONTROL_DASHBOARD_PATH, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH,
      FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, FLOWIE_CONTROL_DASHBOARD_CSS_PATH,
      FLOWIE_CONTROL_DASHBOARD_HTMX_PATH, FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH,
      FLOWIE_CONTROL_DASHBOARD_LOGOUT_PATH};
  if (!config || !config->management.rpc_path[0]) return 0;
  if (strcmp(config->management.rpc_path, FLOWIE_CONTROL_RUNTIME_SESSION_CONTEXT) == 0) return 0;
  if (config->auth.external_https.enabled && !config->auth.enabled) return 0;
  if (config->auth.enabled &&
      strcmp(config->management.rpc_path, FLOWIE_CONTROL_AUTH_HTTP_PATH) == 0)
    return 0;
  if (config->auth.enabled &&
      strcmp(config->management.rpc_path, FLOWIE_CONTROL_ACL_HTTP_PATH) == 0)
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
  if (!config || config->size < sizeof(*config) ||
      config->version != FLOWIE_CONTROL_CONFIG_VERSION || !tls_out)
    return TURBO_EINVAL;
  rc = flowie_control_runtime_env_secret(config->listener.tls.key_password_ref, &password);
  if (rc != TURBO_OK) return rc;
  *tls_out = (turbo_tls_server_config_t){sizeof(*tls_out),
                                         config->listener.tls.cert_file,
                                         config->listener.tls.key_file,
                                         password,
                                         config->listener.tls.client_auth_required
                                             ? config->listener.tls.client_ca_file
                                             : NULL,
                                         NULL,
                                         config->listener.tls.client_auth_required
                                             ? TURBO_TLS_CLIENT_AUTH_REQUIRED
                                             : TURBO_TLS_CLIENT_AUTH_NONE};
  return TURBO_OK;
}

static coro_context_t *
flowie_control_runtime_listener_context_create(const flowie_control_config_t *config) {
  coro_object_pool_config_t pool = CORO_OBJECT_POOL_CONFIG_DEFAULT;
  if (!config ||
      config->listener.coroutine_stack_size <
          FLOWIE_CONTROL_CONFIG_LISTENER_MIN_COROUTINE_STACK_SIZE ||
      config->listener.coroutine_stack_size >
          FLOWIE_CONTROL_CONFIG_LISTENER_MAX_COROUTINE_STACK_SIZE)
    return NULL;
  pool.stack_size = config->listener.coroutine_stack_size;
  return coro_context_create_ex(NULL, &pool);
}

static int flowie_control_runtime_validate_store(const flowie_control_config_t *config) {
  const char *password = NULL;
  int rc;
  if (!config || config->size < sizeof(*config) || config->version != FLOWIE_CONTROL_CONFIG_VERSION)
    return TURBO_EINVAL;
  switch (config->store_provider) {
  case FLOWIE_CONTROL_CONFIG_STORE_SQLITE:
    return TURBO_OK;
  case FLOWIE_CONTROL_CONFIG_STORE_POSTGRESQL:
#if defined(FLOWIE_CONTROL_HAS_PGSQL)
    rc = flowie_control_pgsql_public_conninfo_validate(config->postgresql.conninfo);
    if (rc != TURBO_OK) return rc;
    rc = flowie_control_runtime_env_secret(config->postgresql.password_ref, &password);
    if (rc != TURBO_OK) return rc;
    if (!password || strlen(password) > FLOWIE_CONTROL_CONFIG_PGSQL_CONNINFO_MAX)
      return TURBO_EINVAL;
    return TURBO_OK;
#else
    (void)password;
    (void)rc;
    return TURBO_ENOTSUP;
#endif
  default:
    return TURBO_EINVAL;
  }
}

int flowie_control_runtime_validate(const flowie_control_config_t *config) {
  turbo_tls_server_config_t tls = {0};
  coro_context_t *context = NULL;
  coro_socket_t *socket = NULL;
  int rc;
  if (!flowie_control_runtime_routes_valid(config)) return TURBO_EINVAL;
  if (config->listener.coroutine_stack_size <
          FLOWIE_CONTROL_CONFIG_LISTENER_MIN_COROUTINE_STACK_SIZE ||
      config->listener.coroutine_stack_size >
          FLOWIE_CONTROL_CONFIG_LISTENER_MAX_COROUTINE_STACK_SIZE)
    return TURBO_EINVAL;
  if (config->dashboard_enabled && config->listener.tls.client_auth_required) return TURBO_EINVAL;
  if ((config->auth.external_https.enabled &&
       config->management.login_executor_configured) ||
      (!config->auth.external_https.enabled &&
       (config->management.login_executor_workers == 0u ||
        config->management.login_executor_workers >
            FLOWIE_CONTROL_CONFIG_AUTH_LOCAL_EXECUTOR_MAX_WORKERS ||
        config->management.login_executor_queue_capacity == 0u ||
        config->management.login_executor_queue_capacity >
            FLOWIE_CONTROL_CONFIG_AUTH_LOCAL_EXECUTOR_MAX_QUEUE_CAPACITY ||
        config->management.login_executor_deadline_ms == 0u ||
        config->management.login_executor_deadline_ms >
            FLOWIE_CONTROL_CONFIG_AUTH_LOCAL_EXECUTOR_MAX_DEADLINE_MS)))
    return TURBO_EINVAL;
  if (strcmp(config->bootstrap.domain_id, FLOWIE_CONTROL_SYSTEM_DOMAIN) != 0 ||
      strcmp(config->bootstrap.principal_id, FLOWIE_CONTROL_SYSTEM_ADMIN_DEFAULT_USERNAME) != 0 ||
      strcmp(config->bootstrap.principal_type, "human") != 0 ||
      sizeof(FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD) - 1u <
          FLOWIE_CONTROL_CONFIG_BOOTSTRAP_PASSWORD_MIN ||
      sizeof(FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD) - 1u >
          FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX)
    return TURBO_EINVAL;
  if (config->auth.enabled &&
      ((config->auth.external_https.enabled && config->auth.local_executor.configured) ||
       (!config->auth.external_https.enabled &&
        (config->auth.local_executor.workers == 0u ||
         config->auth.local_executor.workers >
             FLOWIE_CONTROL_CONFIG_AUTH_LOCAL_EXECUTOR_MAX_WORKERS ||
         config->auth.local_executor.queue_capacity == 0u ||
         config->auth.local_executor.queue_capacity >
             FLOWIE_CONTROL_CONFIG_AUTH_LOCAL_EXECUTOR_MAX_QUEUE_CAPACITY ||
         config->auth.local_executor.deadline_ms == 0u ||
         config->auth.local_executor.deadline_ms >
             FLOWIE_CONTROL_CONFIG_AUTH_LOCAL_EXECUTOR_MAX_DEADLINE_MS))))
    return TURBO_EINVAL;
  rc = flowie_control_runtime_validate_store(config);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_runtime_tls_config(config, &tls);
  if (rc != TURBO_OK) return rc;
  context = flowie_control_runtime_listener_context_create(config);
  if (!context) return TURBO_ENOMEM;
  socket = coro_socket_create(context, CORO_SOCKET_TLS);
  if (!socket) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = coro_socket_set_tls_server_config(socket, &tls);
  if (rc == TURBO_OK && config->auth.external_https.enabled) {
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
    rc = flowie_control_runtime_validate_external_https(config);
#else
    rc = TURBO_ENOTSUP;
#endif
  }

done:
  if (socket) coro_socket_destroy(socket);
  coro_context_destroy(context);
  return rc;
}

static int flowie_control_runtime_request_token(
    const Req *request, char output[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  static const char prefix[] = "Bearer ";
  const char *authorization;
  char cookie[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  const char *bearer = NULL;
  size_t bearer_size = 0u;
  int has_cookie;
  int rc = TURBO_EPERM;
  if (output) output[0] = '\0';
  if (!request || !output) return TURBO_EINVAL;
  has_cookie = flowie_control_http_cookie_exact(request, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE,
                                                cookie, sizeof(cookie)) == TURBO_OK;
  if (flowie_control_http_header_exact(request, "Authorization", &authorization) != TURBO_OK)
    authorization = NULL;
  if (authorization && strncmp(authorization, prefix, sizeof(prefix) - 1u) == 0) {
    bearer = authorization + sizeof(prefix) - 1u;
    bearer_size = strnlen(bearer, FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u);
  }
  if (has_cookie && bearer && strcmp(cookie, bearer) != 0) goto done;
  if (bearer && bearer_size == FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE)
    memcpy(output, bearer, bearer_size + 1u);
  else if (has_cookie &&
           strnlen(cookie, sizeof(cookie)) ==
               FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE)
    memcpy(output, cookie, FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u);
  else
    goto done;
  rc = TURBO_OK;

done:
  crypto_wipe(cookie, sizeof(cookie));
  return rc;
}

static int flowie_control_runtime_session_middleware(Req *request, Res *response, Chain *chain) {
  flowie_control_runtime_t *runtime;
  flowie_control_management_session_identity_t *identity;
  char token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  int rc;
  if (!request || !response || !chain || !request->security || !request->app) return 1;
  runtime = (flowie_control_runtime_t *)iris_app_lookup_rpc_context(
      request->app, FLOWIE_CONTROL_RUNTIME_SESSION_CONTEXT);
  if (!runtime) return next(chain, request, response);
  rc = flowie_control_runtime_request_token(request, token);
  if (rc != TURBO_OK) return next(chain, request, response);
  identity = (flowie_control_management_session_identity_t *)calloc(1u, sizeof(*identity));
  if (!identity) {
    crypto_wipe(token, sizeof(token));
    send_json(response, INTERNAL_SERVER_ERROR, "{\"error\":\"Session unavailable\"}");
    return 1;
  }
  identity->size = sizeof(*identity);
  rc = flowie_control_management_session_resolve(runtime->management_sessions, token, identity);
  crypto_wipe(token, sizeof(token));
  if (rc != TURBO_OK) {
    free(identity);
    return next(chain, request, response);
  }
  set_context(request, identity, sizeof(*identity), free);
  request->security->authenticated = true;
  return next(chain, request, response);
}

static int flowie_control_runtime_resolve_caller(void *ctx, const Req *request,
                                                 flowie_control_management_caller_t *caller_out) {
  const flowie_control_management_session_identity_t *identity;
  (void)ctx;
  if (!request || !caller_out || caller_out->size < sizeof(*caller_out))
    return TURBO_EINVAL;
  identity = (const flowie_control_management_session_identity_t *)get_context((Req *)request);
  if (!identity || identity->size < sizeof(*identity)) return TURBO_EPERM;
  caller_out->domain_id = identity->domain_id;
  caller_out->actor = identity->principal_id;
  caller_out->permissions = identity->permissions;
  return TURBO_OK;
}

static int flowie_control_runtime_resolve_session(
    void *ctx, const Req *request, flowie_control_management_caller_t *caller_out,
    char csrf_token_out[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]) {
  const flowie_control_management_session_identity_t *identity;
  (void)ctx;
  if (csrf_token_out) csrf_token_out[0] = '\0';
  if (!request || !caller_out || caller_out->size < sizeof(*caller_out) || !csrf_token_out)
    return TURBO_EINVAL;
  identity = (const flowie_control_management_session_identity_t *)get_context((Req *)request);
  if (!identity || identity->size < sizeof(*identity)) return TURBO_EPERM;
  caller_out->domain_id = identity->domain_id;
  caller_out->actor = identity->principal_id;
  caller_out->permissions = identity->permissions;
  memcpy(csrf_token_out, identity->csrf, sizeof(identity->csrf));
  return TURBO_OK;
}

static int flowie_control_runtime_login(
    void *ctx, const char *domain_id, const char *principal_id, const uint8_t *secret,
    size_t secret_size, const char *remote_address,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  if (!runtime) return TURBO_EINVAL;
  return flowie_control_management_session_login(runtime->management_sessions, domain_id,
                                                 principal_id, secret, secret_size, remote_address,
                                                 token_out);
}

static int flowie_control_runtime_logout(void *ctx, const char *token) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  return runtime ? flowie_control_management_session_revoke(runtime->management_sessions, token)
                 : TURBO_EINVAL;
}

static int flowie_control_runtime_policy_version(void *ctx, const char *domain_id,
                                                 uint64_t *policy_version_out) {
  flowie_control_runtime_t *runtime = (flowie_control_runtime_t *)ctx;
  flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  int rc;
  if (policy_version_out) *policy_version_out = 0u;
  if (!runtime || flowie_control_repository_validate(runtime->repository) != TURBO_OK ||
      !domain_id || !policy_version_out)
    return TURBO_EINVAL;
  rc = runtime->repository->policy->status(runtime->repository->ctx, domain_id, &status);
  if (rc == TURBO_OK) *policy_version_out = status.policy_version;
  return rc;
}

static int flowie_control_runtime_secret_acquire(void *ctx, const char *reference,
                                                 turbo_flow_security_secret_lease_t *lease_out) {
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

static turbo_flow_security_key_provider_t
flowie_control_runtime_key_provider(flowie_control_runtime_t *runtime) {
  turbo_flow_security_key_provider_t provider = TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT;
  provider.ctx = runtime;
  provider.acquire = flowie_control_runtime_secret_acquire;
  provider.release = flowie_control_runtime_secret_release;
  return provider;
}

#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
static void flowie_control_runtime_external_configs(
    const flowie_control_config_t *config, const turbo_flow_security_key_provider_t *key_provider,
    flowie_control_external_https_authenticator_config_t *authenticator_out,
    flowie_control_external_subject_mapper_config_t *mapper_out) {
  const flowie_control_config_external_https_t *external = &config->auth.external_https;
  *authenticator_out = (flowie_control_external_https_authenticator_config_t)
      FLOWIE_CONTROL_EXTERNAL_HTTPS_AUTHENTICATOR_CONFIG_INIT;
  authenticator_out->url = external->url;
  authenticator_out->method = config->auth.method;
  authenticator_out->service_token_ref = external->service_token_ref;
  authenticator_out->timeout_ms = external->timeout_ms;
  authenticator_out->max_response_size = external->max_response_size;
  authenticator_out->max_in_flight = external->max_in_flight;
  authenticator_out->key_provider = *key_provider;
  authenticator_out->tls.ca_file = external->tls.ca_file[0] ? external->tls.ca_file : NULL;
  authenticator_out->tls.client_cert_file =
      external->tls.client_cert_file[0] ? external->tls.client_cert_file : NULL;
  authenticator_out->tls.client_key_file =
      external->tls.client_key_file[0] ? external->tls.client_key_file : NULL;
  authenticator_out->tls.client_key_password_ref =
      external->tls.client_key_password_ref[0] ? external->tls.client_key_password_ref : NULL;
  *mapper_out = (flowie_control_external_subject_mapper_config_t)
      FLOWIE_CONTROL_EXTERNAL_SUBJECT_MAPPER_CONFIG_INIT;
  mapper_out->trusted_issuer = external->trusted_issuer;
  mapper_out->subject_type = external->subject_type;
}

static int flowie_control_runtime_validate_external_https(const flowie_control_config_t *config) {
  turbo_flow_security_key_provider_t key_provider = flowie_control_runtime_key_provider(NULL);
  flowie_control_external_https_authenticator_config_t authenticator_config;
  flowie_control_external_subject_mapper_config_t mapper_config;
  flowie_control_external_https_authenticator_t *authenticator = NULL;
  flowie_control_external_subject_mapper_t *mapper = NULL;
  int rc;
  if (!config->auth.external_https.enabled) return TURBO_OK;
  flowie_control_runtime_external_configs(config, &key_provider, &authenticator_config,
                                          &mapper_config);
  rc = flowie_control_external_https_authenticator_create(&authenticator_config, &authenticator);
  if (rc == TURBO_OK) rc = flowie_control_external_subject_mapper_create(&mapper_config, &mapper);
  flowie_control_external_subject_mapper_destroy(mapper);
  flowie_control_external_https_authenticator_destroy(authenticator);
  return rc;
}

static int
flowie_control_runtime_create_external_https(flowie_control_runtime_t *runtime,
                                             flowie_control_auth_service_config_t *service_config) {
  turbo_flow_security_key_provider_t key_provider = flowie_control_runtime_key_provider(runtime);
  flowie_control_external_https_authenticator_config_t authenticator_config;
  flowie_control_external_subject_mapper_config_t mapper_config;
  int rc;
  if (!runtime->config.auth.external_https.enabled) return TURBO_OK;
  if (runtime->external_https_authenticator && runtime->external_subject_mapper) {
    service_config->external_authenticator =
        flowie_control_external_https_authenticator_interface(
            runtime->external_https_authenticator);
    service_config->external_identity_mapper =
        flowie_control_external_subject_mapper_interface(runtime->external_subject_mapper);
    return TURBO_OK;
  }
  flowie_control_runtime_external_configs(&runtime->config, &key_provider, &authenticator_config,
                                          &mapper_config);
  rc = flowie_control_external_https_authenticator_create(&authenticator_config,
                                                          &runtime->external_https_authenticator);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_external_subject_mapper_create(&mapper_config,
                                                     &runtime->external_subject_mapper);
  if (rc != TURBO_OK) return rc;
  service_config->external_authenticator =
      flowie_control_external_https_authenticator_interface(runtime->external_https_authenticator);
  service_config->external_identity_mapper =
      flowie_control_external_subject_mapper_interface(runtime->external_subject_mapper);
  return TURBO_OK;
}
#endif

static int flowie_control_runtime_create_management_sessions(
    flowie_control_runtime_t *runtime) {
  flowie_control_auth_service_config_t auth_config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_management_session_config_t session_config =
      FLOWIE_CONTROL_MANAGEMENT_SESSION_CONFIG_INIT;
  int rc;
  if (!runtime || !runtime->repository) return TURBO_EINVAL;
  auth_config.repository = runtime->repository;
  auth_config.method = runtime->config.auth.enabled ? runtime->config.auth.method : "password";
  auth_config.principal_ttl_seconds =
      runtime->config.management.session_ttl_seconds >
              FLOWIE_CONTROL_AUTH_MAX_PRINCIPAL_TTL_SECONDS
          ? FLOWIE_CONTROL_AUTH_MAX_PRINCIPAL_TTL_SECONDS
          : runtime->config.management.session_ttl_seconds;
  auth_config.credential_cache.capacity =
      runtime->config.auth.enabled ? runtime->config.auth.credential_cache_capacity : 4096u;
  auth_config.credential_cache.ttl_ms =
      (runtime->config.auth.enabled ? runtime->config.auth.credential_cache_ttl_seconds : 60u) *
      1000u;
  auth_config.principal_cache.capacity = auth_config.credential_cache.capacity;
  auth_config.principal_cache.ttl_ms = auth_config.credential_cache.ttl_ms;
  auth_config.policy_version.ctx = runtime;
  auth_config.policy_version.current = flowie_control_runtime_policy_version;
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
  rc = flowie_control_runtime_create_external_https(runtime, &auth_config);
  if (rc != TURBO_OK) return rc;
#else
  if (runtime->config.auth.external_https.enabled) return TURBO_ENOTSUP;
#endif
  rc = flowie_control_auth_service_create(&auth_config, &runtime->management_auth_service);
  if (rc != TURBO_OK) return rc;
  session_config.repository = runtime->repository;
  session_config.auth_service = runtime->management_auth_service;
  session_config.method = auth_config.method;
  session_config.capacity = runtime->config.management.session_capacity;
  session_config.max_sessions_per_principal =
      runtime->config.management.session_max_sessions_per_principal;
  session_config.ttl_seconds = runtime->config.management.session_ttl_seconds;
  session_config.clock = flowie_control_runtime_clock;
  session_config.clock_ctx = runtime;
  return flowie_control_management_session_store_create(&session_config,
                                                        &runtime->management_sessions);
}

static int flowie_control_runtime_create_auth(flowie_control_runtime_t *runtime) {
  flowie_control_service_credential_config_t credential_config =
      FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT;
  flowie_control_auth_service_config_t service_config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_auth_iris_adapter_config_t adapter_config =
      FLOWIE_CONTROL_AUTH_IRIS_ADAPTER_CONFIG_INIT;
  flowie_control_auth_iris_endpoint_config_t endpoint_config =
      FLOWIE_CONTROL_AUTH_IRIS_ENDPOINT_CONFIG_INIT;
  flowie_control_acl_iris_endpoint_config_t acl_config =
      FLOWIE_CONTROL_ACL_IRIS_ENDPOINT_CONFIG_INIT;
  int rc;
  if (!runtime->config.auth.enabled) return TURBO_OK;
  service_config.repository = runtime->repository;
  service_config.method = runtime->config.auth.method;
  service_config.principal_ttl_seconds = runtime->config.auth.principal_ttl_seconds;
  service_config.credential_cache.capacity = runtime->config.auth.credential_cache_capacity;
  service_config.credential_cache.ttl_ms =
      runtime->config.auth.credential_cache_ttl_seconds * 1000u;
  service_config.principal_cache.capacity = runtime->config.auth.credential_cache_capacity;
  service_config.principal_cache.ttl_ms = runtime->config.auth.credential_cache_ttl_seconds * 1000u;
  service_config.policy_version.ctx = runtime;
  service_config.policy_version.current = flowie_control_runtime_policy_version;
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
  rc = flowie_control_runtime_create_external_https(runtime, &service_config);
  if (rc != TURBO_OK) return rc;
#else
  if (runtime->config.auth.external_https.enabled) return TURBO_ENOTSUP;
#endif
  rc = flowie_control_auth_service_create(&service_config, &runtime->auth_service);
  if (rc != TURBO_OK) return rc;
  credential_config.listener_id = runtime->config.auth.listener_id;
  credential_config.repository = runtime->repository;
  rc = flowie_control_service_credential_resolver_create(
      &credential_config, &runtime->service_credentials);
  if (rc != TURBO_OK) return rc;
  adapter_config.service = runtime->auth_service;
  rc = flowie_control_auth_iris_adapter_create(&adapter_config, &runtime->auth_adapter);
  if (rc != TURBO_OK) return rc;
  endpoint_config.adapter = runtime->auth_adapter;
  endpoint_config.service_credentials = runtime->service_credentials;
  endpoint_config.max_request_body_size = FLOWIE_CONTROL_AUTH_ENDPOINT_BODY_MAX;
  endpoint_config.local_executor_enabled = runtime->config.auth.external_https.enabled ? 0 : 1;
  endpoint_config.local_executor_workers = runtime->config.auth.local_executor.workers;
  endpoint_config.local_executor_queue_capacity =
      runtime->config.auth.local_executor.queue_capacity;
  endpoint_config.local_executor_deadline_ms = runtime->config.auth.local_executor.deadline_ms;
  rc = flowie_control_auth_iris_endpoint_create(&endpoint_config, &runtime->auth_endpoint);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_auth_iris_endpoint_register(runtime->auth_endpoint, runtime->app);
  if (rc != TURBO_OK) return rc;
  acl_config.repository = runtime->repository;
  acl_config.service_credentials = runtime->service_credentials;
  rc = flowie_control_acl_iris_endpoint_create(&acl_config, &runtime->acl_endpoint);
  if (rc != TURBO_OK) return rc;
  return flowie_control_acl_iris_endpoint_register(runtime->acl_endpoint, runtime->app);
}

static int flowie_control_runtime_create_repository(flowie_control_runtime_t *runtime) {
  flowie_control_store_config_t sqlite_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  if (!runtime) return TURBO_EINVAL;
  switch (runtime->config.store_provider) {
  case FLOWIE_CONTROL_CONFIG_STORE_SQLITE:
    sqlite_config.database_path = runtime->config.sqlite_path;
    sqlite_config.busy_timeout_ms = runtime->config.sqlite_busy_timeout_ms;
    {
      int rc = flowie_control_store_open(&sqlite_config, &runtime->store);
      if (rc != TURBO_OK) return rc;
    }
    runtime->repository = flowie_control_store_repository(runtime->store);
    break;
  case FLOWIE_CONTROL_CONFIG_STORE_POSTGRESQL:
#if defined(FLOWIE_CONTROL_HAS_PGSQL)
  {
    flowie_control_pgsql_pool_config_t pgsql_config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    const char *password = NULL;
    int rc = flowie_control_runtime_env_secret(runtime->config.postgresql.password_ref, &password);
    if (rc != TURBO_OK) return rc;
    pgsql_config.database.conninfo = runtime->config.postgresql.conninfo;
    pgsql_config.database.password = password;
    pgsql_config.database.schema_name = runtime->config.postgresql.schema_name;
    pgsql_config.database.connect_timeout_seconds =
        runtime->config.postgresql.connect_timeout_seconds;
    pgsql_config.database.statement_timeout_ms = runtime->config.postgresql.statement_timeout_ms;
    pgsql_config.database.lock_timeout_ms = runtime->config.postgresql.lock_timeout_ms;
    pgsql_config.database.require_tls = 1;
    pgsql_config.database.schema_mode =
        runtime->config.postgresql.schema_mode == FLOWIE_CONTROL_CONFIG_PGSQL_SCHEMA_MIGRATE
            ? FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE
            : FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE;
    pgsql_config.capacity = runtime->config.postgresql.pool_capacity;
    pgsql_config.acquire_timeout_ms = runtime->config.postgresql.acquire_timeout_ms;
    rc = flowie_control_pgsql_repository_create(&pgsql_config, &runtime->pgsql_provider);
    if (rc != TURBO_OK) return rc;
    runtime->repository = flowie_control_pgsql_repository_view(runtime->pgsql_provider);
  } break;
#else
    return TURBO_ENOTSUP;
#endif
  default:
    return TURBO_EINVAL;
  }
  return flowie_control_repository_validate(runtime->repository);
}

int flowie_control_runtime_create(const flowie_control_config_t *config,
                                  flowie_control_runtime_t **out) {
  flowie_control_runtime_t *runtime = NULL;
  flowie_control_management_service_config_t management_config =
      FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_management_rpc_server_config_t rpc_server_config =
      FLOWIE_CONTROL_MANAGEMENT_RPC_SERVER_CONFIG_INIT;
  flowie_control_dashboard_config_t dashboard_config = FLOWIE_CONTROL_DASHBOARD_CONFIG_INIT;
  iris_security_limits_t limits;
  rpc_config_t rpc_config = RPC_DEFAULT_CONFIG();
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      config->version != FLOWIE_CONTROL_CONFIG_VERSION || !out)
    return TURBO_EINVAL;
  rc = flowie_control_runtime_validate(config);
  if (rc != TURBO_OK) return rc;
  runtime = (flowie_control_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  atomic_init(&runtime->listener_started, 0);
  atomic_init(&runtime->listener_shutdown_status, TURBO_OK);
  runtime->config = *config;
  rc = flowie_control_runtime_create_repository(runtime);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_bootstrap_apply(
      runtime->repository, &runtime->config.bootstrap,
      FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD,
      sizeof(FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD) - 1u,
      flowie_control_runtime_clock(NULL));
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_runtime_create_management_sessions(runtime);
  if (rc != TURBO_OK) goto fail;
  management_config.repository = runtime->repository;
  rc = flowie_control_management_service_create(&management_config, &runtime->management_service);
  if (rc != TURBO_OK) goto fail;
  runtime->app = iris_app_create();
  if (!runtime->app) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  if (iris_app_bind_rpc_context(runtime->app, FLOWIE_CONTROL_RUNTIME_SESSION_CONTEXT, runtime) !=
      0) {
    rc = TURBO_EBUSY;
    goto fail;
  }
  iris_app_hook(runtime->app, flowie_control_runtime_session_middleware);
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
  rpc_server_config.external_https_stats = flowie_control_runtime_external_https_stats;
  rpc_server_config.external_https_stats_ctx = runtime;
  rc = flowie_control_management_rpc_server_create(&rpc_server_config, &runtime->management_rpc);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_management_rpc_server_bind(runtime->management_rpc, runtime->app);
  if (rc != TURBO_OK) goto fail;
  if (runtime->config.dashboard_enabled) {
    dashboard_config.service = runtime->management_service;
    dashboard_config.resolve_session = flowie_control_runtime_resolve_session;
    dashboard_config.resolve_session_ctx = runtime;
    dashboard_config.clock = flowie_control_runtime_clock;
    dashboard_config.clock_ctx = runtime;
    dashboard_config.login = flowie_control_runtime_login;
    dashboard_config.logout = flowie_control_runtime_logout;
    dashboard_config.session_ctx = runtime;
    dashboard_config.session_ttl_seconds = runtime->config.management.session_ttl_seconds;
    dashboard_config.login_executor_enabled =
        runtime->config.auth.external_https.enabled ? 0 : 1;
    dashboard_config.login_executor_workers =
        runtime->config.management.login_executor_workers;
    dashboard_config.login_executor_queue_capacity =
        runtime->config.management.login_executor_queue_capacity;
    dashboard_config.login_executor_deadline_ms =
        runtime->config.management.login_executor_deadline_ms;
    rc = flowie_control_dashboard_create(&dashboard_config, &runtime->dashboard);
    if (rc != TURBO_OK) goto fail;
    rc = flowie_control_dashboard_bind(runtime->dashboard, runtime->app);
    if (rc != TURBO_OK) goto fail;
  }
  rc = flowie_control_runtime_create_auth(runtime);
  if (rc != TURBO_OK) goto fail;
  *out = runtime;
  return TURBO_OK;

fail: {
  int cleanup_rc = flowie_control_runtime_destroy(runtime);
  if (cleanup_rc != TURBO_OK) return cleanup_rc;
}
  return rc;
}

int flowie_control_runtime_start(flowie_control_runtime_t *runtime) {
  turbo_tls_server_config_t tls = {0};
  int shutdown_rc;
  int rc;
  if (!runtime || !runtime->app || runtime->listener || runtime->listener_context ||
      runtime->listener_stop_wait || runtime->listener_thread_started ||
      atomic_load_explicit(&runtime->listener_started, memory_order_acquire))
    return TURBO_EINVAL;
  rc = flowie_control_runtime_tls_config(&runtime->config, &tls);
  if (rc != TURBO_OK) return rc;
  runtime->listener_context = flowie_control_runtime_listener_context_create(&runtime->config);
  if (!runtime->listener_context) return TURBO_ENOMEM;
  runtime->listener = iris_server_start_tls_on(
      runtime->app, runtime->listener_context, runtime->config.listener.host,
      runtime->config.listener.port, &tls);
  if (!runtime->listener) {
    coro_context_destroy(runtime->listener_context);
    runtime->listener_context = NULL;
    return TURBO_EIO;
  }
  runtime->listener_stop_wait = coro_wait_create(runtime->listener_context);
  if (!runtime->listener_stop_wait) {
    (void)coro_socket_server_stop(runtime->listener);
    while (!coro_socket_server_is_stopped(runtime->listener))
      (void)coro_context_run(runtime->listener_context, TURBO_RUN_NOWAIT);
    coro_socket_destroy(runtime->listener);
    runtime->listener = NULL;
    coro_context_destroy(runtime->listener_context);
    runtime->listener_context = NULL;
    return TURBO_ENOMEM;
  }
  atomic_store_explicit(&runtime->listener_shutdown_status, TURBO_OK, memory_order_release);
  atomic_store_explicit(&runtime->listener_started, 1, memory_order_release);
  rc = coro_context_spawn(runtime->listener_context,
                          flowie_control_runtime_listener_shutdown, runtime);
  if (rc != TURBO_OK) {
    atomic_store_explicit(&runtime->listener_started, 0, memory_order_release);
    (void)coro_socket_server_stop(runtime->listener);
    while (!coro_socket_server_is_stopped(runtime->listener))
      (void)coro_context_run(runtime->listener_context, TURBO_RUN_NOWAIT);
    coro_socket_destroy(runtime->listener);
    runtime->listener = NULL;
    (void)coro_wait_destroy(runtime->listener_stop_wait);
    runtime->listener_stop_wait = NULL;
    coro_context_destroy(runtime->listener_context);
    runtime->listener_context = NULL;
    return rc;
  }
  rc = turbo_thread_create(&runtime->listener_thread,
                           flowie_control_runtime_listener_thread, runtime);
  if (rc != TURBO_OK) {
    atomic_store_explicit(&runtime->listener_started, 0, memory_order_release);
    (void)coro_wait_interrupt(runtime->listener_stop_wait, TURBO_ECANCELED);
    (void)coro_context_run(runtime->listener_context, TURBO_RUN_DEFAULT);
    shutdown_rc = atomic_load_explicit(&runtime->listener_shutdown_status,
                                       memory_order_acquire);
    if (shutdown_rc != TURBO_OK) rc = shutdown_rc;
    (void)coro_wait_destroy(runtime->listener_stop_wait);
    runtime->listener_stop_wait = NULL;
    coro_context_destroy(runtime->listener_context);
    runtime->listener_context = NULL;
    return rc;
  }
  runtime->listener_thread_started = 1;
  return TURBO_OK;
}

int flowie_control_runtime_stop(flowie_control_runtime_t *runtime) {
  int rc = TURBO_OK;
  if (!runtime) return TURBO_EINVAL;
  if (runtime->listener_thread_started) {
    atomic_store_explicit(&runtime->listener_started, 0, memory_order_release);
    if (runtime->listener_stop_wait)
      (void)coro_wait_interrupt(runtime->listener_stop_wait, TURBO_ECANCELED);
    rc = turbo_thread_join(&runtime->listener_thread);
    if (rc != TURBO_OK) return rc;
    runtime->listener_thread_started = 0;
    rc = atomic_load_explicit(&runtime->listener_shutdown_status, memory_order_acquire);
  }
  if (runtime->listener_stop_wait) {
    int wait_rc = coro_wait_destroy(runtime->listener_stop_wait);
    if (rc == TURBO_OK && wait_rc != TURBO_OK) rc = wait_rc;
    if (wait_rc == TURBO_OK) runtime->listener_stop_wait = NULL;
  }
  if (runtime->listener_context) {
    coro_context_destroy(runtime->listener_context);
    runtime->listener_context = NULL;
  }
  return rc;
}

int flowie_control_runtime_run(flowie_control_runtime_t *runtime) {
  turbo_tls_server_config_t tls = {0};
  int rc;
  if (!runtime || !runtime->app || runtime->listener || runtime->listener_context ||
      runtime->listener_thread_started)
    return TURBO_EINVAL;
  rc = flowie_control_runtime_tls_config(&runtime->config, &tls);
  if (rc != TURBO_OK) return rc;
  return iris_app_listen_tls_on(runtime->app, runtime->config.listener.host,
                                runtime->config.listener.port, &tls) == 0
             ? TURBO_OK
             : TURBO_EIO;
}

int flowie_control_runtime_destroy(flowie_control_runtime_t *runtime) {
  int rc;
  if (!runtime) return TURBO_OK;
  rc = flowie_control_runtime_stop(runtime);
  if (rc != TURBO_OK) return rc;
  flowie_control_acl_iris_endpoint_destroy(runtime->acl_endpoint);
  runtime->acl_endpoint = NULL;
  flowie_control_auth_iris_endpoint_destroy(runtime->auth_endpoint);
  runtime->auth_endpoint = NULL;
  flowie_control_auth_iris_adapter_destroy(runtime->auth_adapter);
  runtime->auth_adapter = NULL;
  flowie_control_service_credential_resolver_destroy(runtime->service_credentials);
  runtime->service_credentials = NULL;
  flowie_control_auth_service_destroy(runtime->auth_service);
  runtime->auth_service = NULL;
  flowie_control_dashboard_destroy(runtime->dashboard);
  runtime->dashboard = NULL;
  flowie_control_management_rpc_server_destroy(runtime->management_rpc);
  runtime->management_rpc = NULL;
  if (runtime->app)
    (void)iris_app_unbind_rpc_context(runtime->app, FLOWIE_CONTROL_RUNTIME_SESSION_CONTEXT,
                                     runtime);
  iris_app_destroy(runtime->app);
  runtime->app = NULL;
  rpc_destroy(runtime->rpc_context);
  runtime->rpc_context = NULL;
  flowie_control_management_session_store_destroy(runtime->management_sessions);
  runtime->management_sessions = NULL;
  flowie_control_auth_service_destroy(runtime->management_auth_service);
  runtime->management_auth_service = NULL;
#if defined(FLOWIE_CONTROL_HAS_EXTERNAL_HTTPS_AUTH)
  flowie_control_external_subject_mapper_destroy(runtime->external_subject_mapper);
  runtime->external_subject_mapper = NULL;
  flowie_control_external_https_authenticator_destroy(runtime->external_https_authenticator);
  runtime->external_https_authenticator = NULL;
#endif
  flowie_control_management_service_destroy(runtime->management_service);
  runtime->management_service = NULL;
#if defined(FLOWIE_CONTROL_HAS_PGSQL)
  rc = flowie_control_pgsql_repository_destroy(runtime->pgsql_provider, 0);
  if (rc != TURBO_OK) return rc;
  runtime->pgsql_provider = NULL;
#else
  rc = TURBO_OK;
#endif
  flowie_control_store_destroy(runtime->store);
  runtime->store = NULL;
  runtime->repository = NULL;
  crypto_wipe(runtime, sizeof(*runtime));
  free(runtime);
  return rc;
}
