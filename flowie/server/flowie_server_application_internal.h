#ifndef FLOWIE_SERVER_APPLICATION_INTERNAL_H
#define FLOWIE_SERVER_APPLICATION_INTERNAL_H

#include "flowie_worker_runtime_internal.h"

#include <stddef.h>

typedef struct flowie_server_application_s flowie_server_application_t;
typedef struct flowie_cluster_generation_config_s flowie_cluster_generation_config_t;

enum {
  FLOWIE_SERVER_STORAGE_BACKEND_PLUGIN_MAX = 8u,
  FLOWIE_SERVER_APPLICATION_ERROR_SUBJECT_MAX = 260u,
  FLOWIE_SERVER_APPLICATION_ERROR_MESSAGE_MAX = 512u
};

#define FLOWIE_SERVER_DEFAULT_PROTOCOL_STORE_PATH "flowie-protocol.sqlite3"

typedef struct flowie_server_application_config_s {
  size_t size;
  const char *profile;
  const char *config_path;
  const char *graph_path;
  const char *protocol_store_path;
  const char *const *storage_backend_plugin_paths;
  size_t storage_backend_plugin_path_count;
  int require_security;
  const char *control_config_path;
  /** Optional embedded-cluster endpoint lane and binding; both are borrowed. */
  const turbo_flow_coronet_execution_binding_t *endpoint_execution;
  const flowie_endpoint_cluster_binding_t *endpoint_cluster;
  /** Optional trusted PROXY v1/v2 policy, borrowed during application creation. */
  const flowie_endpoint_proxy_binding_t *endpoint_proxy;
  /** Optional root-owned embedded cluster generation; excludes borrowed endpoint cluster ports. */
  const flowie_cluster_generation_config_t *cluster;
} flowie_server_application_config_t;

#define FLOWIE_SERVER_APPLICATION_CONFIG_INIT                                                      \
  {sizeof(flowie_server_application_config_t),                                                     \
   "flowie",                                                                                       \
   NULL,                                                                                           \
   NULL,                                                                                           \
   FLOWIE_SERVER_DEFAULT_PROTOCOL_STORE_PATH,                                                      \
   NULL,                                                                                           \
   0u,                                                                                             \
   0,                                                                                              \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef enum flowie_server_application_error_detail_e {
  FLOWIE_SERVER_APPLICATION_ERROR_NONE = 0,
  FLOWIE_SERVER_APPLICATION_ERROR_WORKER,
  FLOWIE_SERVER_APPLICATION_ERROR_STORAGE_PLUGIN,
  FLOWIE_SERVER_APPLICATION_ERROR_CONTROL_CONFIG,
  FLOWIE_SERVER_APPLICATION_ERROR_CONTROL_RUNTIME,
  FLOWIE_SERVER_APPLICATION_ERROR_CLUSTER_GENERATION
} flowie_server_application_error_detail_t;

typedef struct flowie_server_application_error_s {
  size_t size;
  const char *operation;
  int status;
  flowie_server_application_error_detail_t detail;
  flowie_worker_error_t worker;
  char subject[FLOWIE_SERVER_APPLICATION_ERROR_SUBJECT_MAX];
  char message[FLOWIE_SERVER_APPLICATION_ERROR_MESSAGE_MAX];
} flowie_server_application_error_t;

#define FLOWIE_SERVER_APPLICATION_ERROR_INIT                                                       \
  {sizeof(flowie_server_application_error_t),                                                      \
   NULL,                                                                                           \
   TURBO_OK,                                                                                       \
   FLOWIE_SERVER_APPLICATION_ERROR_NONE,                                                           \
   FLOWIE_WORKER_ERROR_INIT,                                                                       \
   {0},                                                                                            \
   {0}}

/**
 * Compose storage, product adapters, HTTPS Auth/ACL factories, one prepared worker, and an
 * optional embedded Control runtime configuration, and one optional owned cluster generation.
 */
int flowie_server_application_create(const flowie_server_application_config_t *config,
                                     flowie_server_application_t **out,
                                     flowie_server_application_error_t *error);

/** Start and stop cluster dependencies, Control, and the prepared MQTT worker. Calls are
 * serialized. */
int flowie_server_application_start(flowie_server_application_t *application,
                                    flowie_server_application_error_t *error);
int flowie_server_application_stop(flowie_server_application_t *application,
                                   flowie_server_application_error_t *error);

/** Stop if necessary and release the complete server generation. */
int flowie_server_application_destroy(flowie_server_application_t *application,
                                      flowie_server_application_error_t *error);

#endif /* FLOWIE_SERVER_APPLICATION_INTERNAL_H */
