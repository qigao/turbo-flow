#ifndef FLOWIE_CONTROL_CONFIG_INTERNAL_H
#define FLOWIE_CONTROL_CONFIG_INTERNAL_H

#include "flowie_control_security_limits_internal.h"
#include "turbo_flow_security.h"
#include "turbo_fs.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_CONFIG_VERSION 1u
#define FLOWIE_CONTROL_CONFIG_HOST_MAX 255u
#define FLOWIE_CONTROL_CONFIG_ROUTE_MAX 127u
#define FLOWIE_CONTROL_CONFIG_SECRET_REF_MAX 1024u
#define FLOWIE_CONTROL_CONFIG_ERROR_PATH_MAX 255u
#define FLOWIE_CONTROL_CONFIG_ERROR_MESSAGE_MAX 255u
#define FLOWIE_CONTROL_CONFIG_MAX_ADMIN_BINDINGS 32u
#define FLOWIE_CONTROL_CONFIG_AUTH_CACHE_CAPACITY_MAX 4096u
#define FLOWIE_CONTROL_CONFIG_AUTH_CACHE_TTL_SECONDS_MAX 60u

typedef struct flowie_control_config_error_s {
  size_t size;
  int status;
  char path[FLOWIE_CONTROL_CONFIG_ERROR_PATH_MAX + 1u];
  char message[FLOWIE_CONTROL_CONFIG_ERROR_MESSAGE_MAX + 1u];
} flowie_control_config_error_t;

#define FLOWIE_CONTROL_CONFIG_ERROR_INIT {sizeof(flowie_control_config_error_t), 0, {0}, {0}}

typedef struct flowie_control_config_tls_s {
  char cert_file[TURBO_FS_MAX_PATH];
  char key_file[TURBO_FS_MAX_PATH];
  char client_ca_file[TURBO_FS_MAX_PATH];
  char key_password_ref[FLOWIE_CONTROL_CONFIG_SECRET_REF_MAX + 1u];
} flowie_control_config_tls_t;

typedef struct flowie_control_config_limits_s {
  size_t max_header_name_length;
  size_t max_header_value_length;
  size_t max_url_length;
  size_t max_cookie_name_length;
  size_t max_cookie_value_length;
  size_t max_json_depth;
  size_t max_log_message_length;
  size_t max_request_body_size;
  int max_headers_count;
} flowie_control_config_limits_t;

typedef struct flowie_control_config_listener_s {
  char host[FLOWIE_CONTROL_CONFIG_HOST_MAX + 1u];
  uint16_t port;
  flowie_control_config_tls_t tls;
  flowie_control_config_limits_t limits;
} flowie_control_config_listener_t;

typedef struct flowie_control_config_admin_binding_s {
  char peer_certificate_sha256[FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u];
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
} flowie_control_config_admin_binding_t;

typedef struct flowie_control_config_management_s {
  char rpc_path[FLOWIE_CONTROL_CONFIG_ROUTE_MAX + 1u];
  size_t rpc_max_request_size;
  size_t admin_binding_count;
  flowie_control_config_admin_binding_t admin_bindings[FLOWIE_CONTROL_CONFIG_MAX_ADMIN_BINDINGS];
} flowie_control_config_management_t;

typedef struct flowie_control_config_auth_s {
  int enabled;
  char listener_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char service_token_ref[FLOWIE_CONTROL_CONFIG_SECRET_REF_MAX + 1u];
  uint64_t principal_ttl_seconds;
  size_t credential_cache_capacity;
  uint64_t credential_cache_ttl_seconds;
  size_t binding_count;
  struct {
    char peer_certificate_sha256[FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u];
    char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  } bindings[FLOWIE_CONTROL_AUTH_MAX_BINDINGS];
} flowie_control_config_auth_t;

typedef struct flowie_control_config_s {
  size_t size;
  uint32_t version;
  flowie_control_config_listener_t listener;
  char sqlite_path[TURBO_FS_MAX_PATH];
  int sqlite_busy_timeout_ms;
  flowie_control_config_management_t management;
  int dashboard_enabled;
  flowie_control_config_auth_t auth;
} flowie_control_config_t;

#define FLOWIE_CONTROL_CONFIG_INIT                                                                \
  {sizeof(flowie_control_config_t),                                                               \
   FLOWIE_CONTROL_CONFIG_VERSION,                                                                 \
   {{0}, 8443u, {{0}, {0}, {0}, {0}}, {128u, 4096u, 2048u, 128u, 4096u, 32u, 2048u, 65536u, 64}}, \
   {0},                                                                                           \
   1000,                                                                                          \
   {{0}, 65536u, 0u, {{0}}},                                                                     \
   1,                                                                                             \
   {0, {0}, {0}, {0}, 300u, 4096u, 60u, 0u, {{0}}}}

int flowie_control_config_parse_yaml(const char *yaml, size_t yaml_size,
                                     flowie_control_config_t *out,
                                     flowie_control_config_error_t *error);
int flowie_control_config_load(const char *path, flowie_control_config_t *out,
                               flowie_control_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif
