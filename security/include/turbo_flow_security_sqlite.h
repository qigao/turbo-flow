#ifndef TURBO_FLOW_SECURITY_SQLITE_H
#define TURBO_FLOW_SECURITY_SQLITE_H

#include "turbo_flow_security.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SECURITY_SQLITE_API_VERSION 1u
#define TURBO_FLOW_SECURITY_SQLITE_BACKEND "sqlite"
#define TURBO_FLOW_SECURITY_SQLITE_NAMESPACE_MAX 255u
#define TURBO_FLOW_SECURITY_SQLITE_DEFAULT_MAX_RULES 4096u

typedef struct turbo_flow_security_sqlite_provider_s turbo_flow_security_sqlite_provider_t;

typedef struct turbo_flow_security_sqlite_config_s {
  size_t size;
  uint32_t api_version;
  const char *database_path;
  const char *namespace_name;
  int busy_timeout_ms;
  size_t max_rules;
} turbo_flow_security_sqlite_config_t;

#define TURBO_FLOW_SECURITY_SQLITE_CONFIG_INIT                                                     \
  {sizeof(turbo_flow_security_sqlite_config_t),                                                    \
   TURBO_FLOW_SECURITY_SQLITE_API_VERSION,                                                         \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0,                                                                                              \
   TURBO_FLOW_SECURITY_SQLITE_DEFAULT_MAX_RULES}

/**
 * Open an embedded, file-backed ACL control store. Database details remain inside this module.
 * `:memory:` is rejected because loads and publishes use independent SQLite connections.
 */
CXX_C_API int
turbo_flow_security_sqlite_provider_create(const turbo_flow_security_sqlite_config_t *config,
                                           turbo_flow_security_sqlite_provider_t **out);

/** Create from one strict `kind: acl_provider`, `backend: sqlite` channel. */
CXX_C_API int turbo_flow_security_sqlite_provider_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_security_sqlite_provider_t **out, turbo_flow_config_error_t *error);

/** Borrowed policy interface valid until provider destruction. */
CXX_C_API const turbo_flow_security_policy_provider_t *
turbo_flow_security_sqlite_provider_interface(
    const turbo_flow_security_sqlite_provider_t *provider);

/**
 * Transactionally replace the namespace's published ACL generation.
 * Versions must increase; readers observe either the previous or complete new bundle.
 */
CXX_C_API int
turbo_flow_security_sqlite_provider_publish(turbo_flow_security_sqlite_provider_t *provider,
                                            const turbo_flow_security_policy_bundle_t *bundle);

CXX_C_API void
turbo_flow_security_sqlite_provider_destroy(turbo_flow_security_sqlite_provider_t *provider);

/** Factory registered by a product composition root under the exact `sqlite` backend. */
CXX_C_API const turbo_flow_security_policy_provider_factory_t *
turbo_flow_security_sqlite_provider_factory(void);

#ifdef __cplusplus
}
#endif

#endif
