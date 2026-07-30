#ifndef TURBO_FLOW_CONFIG_H
#define TURBO_FLOW_CONFIG_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CONFIG_PATH_MAX 255u
#define TURBO_FLOW_CONFIG_MESSAGE_MAX 255u

typedef struct turbo_flow_resolved_config_s turbo_flow_resolved_config_t;

struct turbo_flow_config_error_s;

typedef int (*turbo_flow_product_adapter_register_fn)(void *ctx, turbo_flow_t *flow,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      const char *adapter_name,
                                                      struct turbo_flow_config_error_s *error);

typedef int (*turbo_flow_product_resource_register_fn)(void *ctx, turbo_flow_t *flow,
                                                       const turbo_flow_resolved_config_t *resolved,
                                                       const char *resource_name,
                                                       struct turbo_flow_config_error_s *error);

/** One trusted native provider for a resolved `adapters.<name>.kind`. */
typedef struct turbo_flow_product_adapter_provider_s {
  size_t size;
  const char *kind;
  turbo_flow_product_adapter_register_fn register_adapter;
  void *ctx;
} turbo_flow_product_adapter_provider_t;

#define TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT                                                   \
  {sizeof(turbo_flow_product_adapter_provider_t), NULL, NULL, NULL}

/** One trusted processor/resource provider for a resolved `channels.<name>.kind`. */
typedef struct turbo_flow_product_resource_provider_s {
  size_t size;
  const char *kind;
  turbo_flow_product_resource_register_fn register_resource;
  void *ctx;
} turbo_flow_product_resource_provider_t;

#define TURBO_FLOW_PRODUCT_RESOURCE_PROVIDER_INIT                                                  \
  {sizeof(turbo_flow_product_resource_provider_t), NULL, NULL, NULL}

/** Caller-owned provider catalog used synchronously during one product build. */
typedef struct turbo_flow_product_provider_registry_s {
  size_t size;
  const turbo_flow_product_adapter_provider_t *adapter_providers;
  size_t adapter_provider_count;
  const turbo_flow_product_resource_provider_t *resource_providers;
  size_t resource_provider_count;
} turbo_flow_product_provider_registry_t;

#define TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT                                                  \
  {sizeof(turbo_flow_product_provider_registry_t), NULL, 0u, NULL, 0u}

typedef enum turbo_flow_config_value_type_e {
  TURBO_FLOW_CONFIG_NULL = 0,
  TURBO_FLOW_CONFIG_BOOL,
  TURBO_FLOW_CONFIG_NUMBER,
  TURBO_FLOW_CONFIG_STRING,
  TURBO_FLOW_CONFIG_ARRAY,
  TURBO_FLOW_CONFIG_OBJECT
} turbo_flow_config_value_type_t;

/** Borrowed immutable adapter projection from one resolved snapshot. */
typedef struct turbo_flow_resolved_adapter_view_s {
  size_t size;
  const char *name;
  const char *kind;
  const void *config;
} turbo_flow_resolved_adapter_view_t;

#define TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT                                                      \
  {sizeof(turbo_flow_resolved_adapter_view_t), NULL, NULL, NULL}

/** Borrowed immutable channel projection from one resolved snapshot. */
typedef struct turbo_flow_resolved_channel_view_s {
  size_t size;
  const char *name;
  const char *kind;
  const void *config;
} turbo_flow_resolved_channel_view_t;

#define TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT                                                      \
  {sizeof(turbo_flow_resolved_channel_view_t), NULL, NULL, NULL}

typedef struct turbo_flow_config_error_s {
  size_t size;
  int status;
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  char message[TURBO_FLOW_CONFIG_MESSAGE_MAX + 1u];
} turbo_flow_config_error_t;

#define TURBO_FLOW_CONFIG_ERROR_INIT {sizeof(turbo_flow_config_error_t), TURBO_OK, {0}, {0}}

/**
 * Resolve one human-authored YAML document into canonical immutable JSON.
 *
 * Accepted top-level keys are `version`, `runtime`, `profiles`, `fragments`,
 * `channels`, and `adapters`. `runtime.ingress` configures the Flow-owned
 * bounded async source ingress; omitted values are expanded to the public Flow
 * defaults in the resolved snapshot. A channel is a named `{kind, config}`
 * resource shared by its adapters. Fragment categories are `connection`,
 * `timer`, `thread`, and `coro`. Profiles map parameters to concrete adapter or
 * channel names; a name present in both scopes is rejected as ambiguous.
 * Fragment/config field collisions are rejected rather than applying an
 * implicit override order.
 */
CXX_C_API int turbo_flow_config_resolve_yaml(const char *yaml, size_t yaml_len,
                                             turbo_flow_resolved_config_t **out,
                                             turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_resolved_config_destroy(turbo_flow_resolved_config_t *config);

/** Borrowed canonical JSON valid until `config` is destroyed. */
CXX_C_API const char *turbo_flow_resolved_config_json(const turbo_flow_resolved_config_t *config,
                                                      size_t *json_len);

/**
 * Copy the process-level bounded async ingress configuration.
 *
 * `ingress` must be initialized with `TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT`.
 * The returned values are always complete, including resolver-expanded
 * defaults when `runtime.ingress` or one of its fields was omitted.
 */
CXX_C_API int
turbo_flow_resolved_config_runtime_ingress(const turbo_flow_resolved_config_t *config,
                                           turbo_flow_async_ingress_config_t *ingress);

/**
 * Resolve one profile parameter to its concrete adapter name.
 *
 * The returned string is borrowed from the immutable resolved snapshot and is
 * valid until `config` is destroyed. Unknown profiles, parameters, or adapter
 * references return TURBO_ENOENT.
 */
CXX_C_API int turbo_flow_resolved_config_profile_adapter(const turbo_flow_resolved_config_t *config,
                                                         const char *profile, const char *parameter,
                                                         const char **adapter_name);

/**
 * Resolve an optional profile adapter parameter.
 *
 * A missing parameter returns TURBO_OK with `adapter_name` set to NULL. Unknown
 * profiles and references that resolve to a channel instead of an adapter fail.
 */
CXX_C_API int turbo_flow_resolved_config_profile_adapter_optional(
    const turbo_flow_resolved_config_t *config, const char *profile, const char *parameter,
    const char **adapter_name);

/** Resolve one profile parameter to a concrete channel resource name. */
CXX_C_API int turbo_flow_resolved_config_profile_channel(const turbo_flow_resolved_config_t *config,
                                                         const char *profile, const char *parameter,
                                                         const char **channel_name);

/**
 * Resolve an optional profile channel parameter.
 *
 * A missing parameter returns TURBO_OK with `channel_name` set to NULL. Unknown
 * profiles and references that resolve to an adapter instead of a channel fail.
 */
CXX_C_API int turbo_flow_resolved_config_profile_channel_optional(
    const turbo_flow_resolved_config_t *config, const char *profile, const char *parameter,
    const char **channel_name);

/** Project one named channel without exposing the resolver's JSON representation. */
CXX_C_API int turbo_flow_resolved_config_channel(const turbo_flow_resolved_config_t *config,
                                                 const char *name,
                                                 turbo_flow_resolved_channel_view_t *view);

/** Read one borrowed string from a resolved channel config. */
CXX_C_API int turbo_flow_resolved_channel_get_string(const turbo_flow_resolved_channel_view_t *view,
                                                     const char *field, const char **value);

/**
 * Preflight a host build's enabled adapter kinds before any adapter projection.
 *
 * Only adapter names and `kind` fields are inspected. A kind absent from the
 * host-provided set returns `TURBO_ENOTSUP`, so disabled components never parse
 * or act on their config. Run this before module registration on a fresh flow.
 */
CXX_C_API int turbo_flow_resolved_config_preflight_adapter_kinds(
    const turbo_flow_resolved_config_t *config, const char *const *enabled_kinds,
    size_t enabled_kind_count, turbo_flow_config_error_t *error);

/**
 * Validate a trusted product provider registry against every resolved adapter kind.
 *
 * This performs no provider callback and no Flow mutation. Duplicate, malformed, or missing
 * providers fail before any native resource is created, so hosts can call it immediately after
 * YAML resolution. Resource providers are validated structurally here and selected after Graph
 * parsing, because unused channels are valid configuration resources.
 *
 * @param config Borrowed immutable resolved YAML snapshot.
 * @param registry Borrowed provider descriptors and callback contexts.
 * @param error Caller-owned structured error initialized with TURBO_FLOW_CONFIG_ERROR_INIT.
 * @return TURBO_OK on success; TURBO_EINVAL for malformed descriptors, TURBO_EALREADY for a
 * duplicate provider kind, TURBO_ENOTSUP for an adapter kind without a provider, or TURBO_EPROTO
 * for an invalid resolved snapshot.
 */
CXX_C_API int turbo_flow_product_preflight(const turbo_flow_resolved_config_t *config,
                                           const turbo_flow_product_provider_registry_t *registry,
                                           turbo_flow_config_error_t *error);

/**
 * Register the resources and adapters explicitly referenced by a parsed Graph.
 *
 * Resource callbacks run before adapter callbacks. Each distinct resource or adapter name is
 * registered once even when multiple nodes reference it. The registry and callback contexts are
 * borrowed only for this call. A callback may create native state and mutate `flow`; after any
 * failure the caller must discard that Flow generation and destroy its provider-owned resources.
 *
 * @param flow Parsed, caller-owned Flow generation.
 * @param config Borrowed immutable resolved YAML snapshot used to parse provider config.
 * @param registry Borrowed provider descriptors and callback contexts.
 * @param error Caller-owned structured error initialized with TURBO_FLOW_CONFIG_ERROR_INIT.
 * @return TURBO_OK on success; TURBO_EINVAL when the Graph is not parsed or a provider rejects its
 * binding, TURBO_ENOENT for a missing referenced adapter/channel, TURBO_ENOTSUP for a missing
 * resource provider, or the exact provider callback status.
 *
 * Typical order is resolve YAML, preflight, parse Graph, assemble Graph providers, then compile.
 */
CXX_C_API int
turbo_flow_product_assemble_graph(turbo_flow_t *flow, const turbo_flow_resolved_config_t *config,
                                  const turbo_flow_product_provider_registry_t *registry,
                                  turbo_flow_config_error_t *error);

/**
 * Resolve one adapter without exposing TurboUtils JSON implementation types.
 *
 * All returned pointers are borrowed from `config` and remain valid until the
 * resolved snapshot is destroyed. Accessors distinguish a missing field
 * (`TURBO_ENOENT`) from a field with the wrong type (`TURBO_EINVAL`).
 */
CXX_C_API int turbo_flow_resolved_config_adapter(const turbo_flow_resolved_config_t *config,
                                                 const char *adapter_name,
                                                 turbo_flow_resolved_adapter_view_t *view);

CXX_C_API size_t
turbo_flow_resolved_adapter_field_count(const turbo_flow_resolved_adapter_view_t *view);
CXX_C_API const char *
turbo_flow_resolved_adapter_field_name(const turbo_flow_resolved_adapter_view_t *view,
                                       size_t index);
CXX_C_API int turbo_flow_resolved_adapter_field_type(const turbo_flow_resolved_adapter_view_t *view,
                                                     const char *field,
                                                     turbo_flow_config_value_type_t *type);
CXX_C_API int turbo_flow_resolved_adapter_get_string(const turbo_flow_resolved_adapter_view_t *view,
                                                     const char *field, const char **value);
CXX_C_API int turbo_flow_resolved_adapter_get_bool(const turbo_flow_resolved_adapter_view_t *view,
                                                   const char *field, int *value);
/**
 * Read an unsigned 64-bit field.
 *
 * JSON-safe integer numbers up to 2^53-1 are accepted directly. Use a string
 * containing only decimal digits for larger values up to UINT64_MAX.
 */
CXX_C_API int turbo_flow_resolved_adapter_get_u64(const turbo_flow_resolved_adapter_view_t *view,
                                                  const char *field, uint64_t *value);
/**
 * Read a signed 64-bit field.
 *
 * JSON-safe integer numbers in the range -(2^53-1)..2^53-1 are accepted
 * directly. Use a decimal string with an optional leading minus sign for the
 * full INT64_MIN..INT64_MAX range.
 */
CXX_C_API int turbo_flow_resolved_adapter_get_i64(const turbo_flow_resolved_adapter_view_t *view,
                                                  const char *field, int64_t *value);
CXX_C_API int turbo_flow_resolved_adapter_array_size(const turbo_flow_resolved_adapter_view_t *view,
                                                     const char *field, size_t *size);
CXX_C_API int
turbo_flow_resolved_adapter_array_string_at(const turbo_flow_resolved_adapter_view_t *view,
                                            const char *field, size_t index, const char **value);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CONFIG_H */
