#ifndef TURBO_FLOW_PROVIDER_H
#define TURBO_FLOW_PROVIDER_H

#include "turbo_flow.h"
#include "turbo_flow_diagnostic.h"
#include "turbo_flow_export.h"

#include <cmeta/interface.h>
#include <data_bind.h>
#include <data_bind_native_binding.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID "turbo_flow.provider_factory"
enum { TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION = 2u };

/**
 * Generated DataBind contract required to bind one provider's .flow literals.
 *
 * Both pointers are provider-module owned and may only be used while the host
 * holds that provider's Salts Plugin lease.
 */
typedef DataBindStatus (*turbo_flow_provider_codec_factory_fn)(
    DataBind **out, DataBindError *error);

typedef struct turbo_flow_provider_config_contract_v1_s {
  size_t size;
  turbo_flow_provider_codec_factory_fn codec_factory;
  const DataBindMessageNativeArtifact *message_artifact;
} turbo_flow_provider_config_contract_v1_t;

#define TURBO_FLOW_PROVIDER_CONFIG_CONTRACT_V1_INIT \
  {sizeof(turbo_flow_provider_config_contract_v1_t), NULL, NULL}

/**
 * Exact deployment-resource Interface requirement declared by a provider.
 *
 * A zero/NULL requirement means the provider does not consume a deployment
 * resource. Otherwise the generation must resolve one named resource through
 * #239, acquire its Salts Plugin lease, and require the exact Interface before
 * provider preflight.
 */
typedef struct turbo_flow_provider_resource_requirement_v1_s {
  size_t size;
  const char *contract_id;
  uint32_t contract_version;
  uint64_t required_capabilities;
  const cmeta_interface_desc *expected_interface;
} turbo_flow_provider_resource_requirement_v1_t;

#define TURBO_FLOW_PROVIDER_RESOURCE_REQUIREMENT_V1_INIT \
  {sizeof(turbo_flow_provider_resource_requirement_v1_t), NULL, 0u, 0u, NULL}

/**
 * Immutable provider-factory metadata returned by contract().
 *
 * The config contract is mandatory in the current authoring model. Resource is
 * optional and uses the canonical Salts Plugin/CMeta Interface identity.
 */
typedef struct turbo_flow_provider_contract_v1_s {
  size_t size;
  turbo_flow_provider_config_contract_v1_t config;
  turbo_flow_provider_resource_requirement_v1_t resource;
} turbo_flow_provider_contract_v1_t;

#define TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT \
  {sizeof(turbo_flow_provider_contract_v1_t), \
   TURBO_FLOW_PROVIDER_CONFIG_CONTRACT_V1_INIT, \
   TURBO_FLOW_PROVIDER_RESOURCE_REQUIREMENT_V1_INIT}

/** Already-bound, already-validated provider config borrowed for a callback. */
typedef struct turbo_flow_provider_config_view_v1_s {
  size_t size;
  const char *type_name;
  const cmeta_data_desc *data;
  const void *value;
  size_t value_bytes;
} turbo_flow_provider_config_view_v1_t;

#define TURBO_FLOW_PROVIDER_CONFIG_VIEW_V1_INIT \
  {sizeof(turbo_flow_provider_config_view_v1_t), NULL, NULL, NULL, 0u}

/**
 * Already-resolved deployment resource Interface borrowed for a callback.
 *
 * reference_name is the exact .flow authoring reference used for Graph binding.
 * identity/export_id are resolved non-secret deployment diagnostics and may differ
 * from reference_name. interface_value points at the exact mutable {self,vtable}
 * handle published by the resource's Salts Plugin INTERFACE export. Generation
 * owns the corresponding plugin lease.
 */
typedef struct turbo_flow_provider_resource_view_v1_s {
  size_t size;
  const char *reference_name;
  const char *identity;
  const char *export_id;
  const cmeta_interface_desc *interface_desc;
  void *interface_value;
} turbo_flow_provider_resource_view_v1_t;

#define TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT \
  {sizeof(turbo_flow_provider_resource_view_v1_t), NULL, NULL, NULL, NULL, NULL}

/** One explicit .flow provider instance passed to preflight/materialize. */
typedef struct turbo_flow_provider_instance_v1_s {
  size_t size;
  const char *instance_name;
  turbo_flow_provider_config_view_v1_t config;
  const turbo_flow_provider_resource_view_v1_t *resource;
} turbo_flow_provider_instance_v1_t;

#define TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT \
  {sizeof(turbo_flow_provider_instance_v1_t), NULL, \
   TURBO_FLOW_PROVIDER_CONFIG_VIEW_V1_INIT, NULL}

enum { TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_VERSION = 1u };

/**
 * One provider-owned adapter materialization bound to exact parsed stages.
 *
 * provider_identity is the visible .flow adapter/provider identity. stage_names
 * are exact parsed stage/source instance names and are never rewritten into
 * registry aliases. A successful call creates one provider-scoped adapter
 * registration and binds every listed stage to that exact registration.
 *
 * At most one of async_terminal_ops/async_emit_ops may be supplied.
 * managed_owner_name and managed_boundary_ops are either both NULL or both
 * present. Callback code and ctx remain provider-owned until Graph registry
 * teardown, matching the existing adapter registration lifetime.
 */
typedef struct turbo_flow_provider_adapter_registration_v1_s {
  size_t size;
  uint32_t version;
  const char *provider_identity;
  const char *const *stage_names;
  size_t stage_count;
  const turbo_flow_adapter_ops_t *adapter_ops;
  const turbo_flow_async_terminal_adapter_ops_t *async_terminal_ops;
  const turbo_flow_async_emit_adapter_ops_t *async_emit_ops;
  const turbo_flow_adapter_schema_t *schema;
  const char *managed_owner_name;
  const turbo_flow_managed_boundary_provider_ops_t *managed_boundary_ops;
  void *ctx;
} turbo_flow_provider_adapter_registration_v1_t;

#define TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT \
  {sizeof(turbo_flow_provider_adapter_registration_v1_t), \
   TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_VERSION, NULL, NULL, 0u, \
   NULL, NULL, NULL, NULL, NULL, NULL, NULL}

/**
 * Atomically register one provider-scoped adapter and bind exact stages.
 *
 * Provider-scoped registrations are intentionally excluded from legacy global
 * adapter-name lookup. Compile resolves an explicit stage binding first and
 * falls back to the legacy global registry only when no explicit binding exists.
 */
TURBO_FLOW_C_API int turbo_flow_provider_adapter_register(
    turbo_flow_t *flow,
    const turbo_flow_provider_adapter_registration_v1_t *registration);

enum {
  TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD = UINT64_C(1) << 0,
  TURBO_FLOW_RUNTIME_OWNER_THREAD_SAFE = UINT64_C(1) << 1,
  TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL = UINT64_C(1) << 2
};

#define TURBO_FLOW_RUNTIME_OWNER_METHODS(X, I) \
  X(I, R1, int, quiesce, uint64_t, timeout_ms) \
  X(I, R1, int, drain, uint64_t, timeout_ms) \
  X(I, R0, int, shutdown, _) \
  X(I, R1, int, poll, uint32_t, timeout_ms) \
  X(I, D0, void, destroy, _)

CMETA_INTERFACE(turbo_flow_runtime_owner, TURBO_FLOW_RUNTIME_OWNER_METHODS);

static inline int
turbo_flow_runtime_owner_contract_valid(const turbo_flow_runtime_owner *owner) {
  const uint64_t capabilities = turbo_flow_runtime_owner_capabilities(owner);
  const uint64_t threading =
      capabilities & (TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
                      TURBO_FLOW_RUNTIME_OWNER_THREAD_SAFE);
  const uint64_t known =
      TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
      TURBO_FLOW_RUNTIME_OWNER_THREAD_SAFE |
      TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL;
  return turbo_flow_runtime_owner_valid(owner) &&
         (capabilities & ~known) == 0u &&
         (threading == TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD ||
          threading == TURBO_FLOW_RUNTIME_OWNER_THREAD_SAFE);
}

#define TURBO_FLOW_PROVIDER_FACTORY_METHODS(X, I) \
  X(I, R1, int, contract, turbo_flow_provider_contract_v1_t *, contract_out) \
  X(I, R2, int, preflight, const turbo_flow_provider_instance_v1_t *, instance, \
    turbo_flow_config_error_t *, error) \
  X(I, R4, int, materialize, turbo_flow_t *, flow, \
    const turbo_flow_provider_instance_v1_t *, instance, \
    turbo_flow_runtime_owner *, owner_out, turbo_flow_config_error_t *, error)

CMETA_INTERFACE(turbo_flow_provider_factory, TURBO_FLOW_PROVIDER_FACTORY_METHODS);

static inline int
turbo_flow_provider_config_contract_valid(
    const turbo_flow_provider_config_contract_v1_t *config) {
  return config != NULL && config->size == sizeof(*config) &&
         config->codec_factory != NULL &&
         data_bind_message_native_artifact_valid(config->message_artifact);
}

static inline int
turbo_flow_provider_resource_requirement_valid(
    const turbo_flow_provider_resource_requirement_v1_t *resource) {
  if (resource == NULL || resource->size != sizeof(*resource)) return 0;
  if (resource->contract_id == NULL && resource->contract_version == 0u &&
      resource->required_capabilities == 0u &&
      resource->expected_interface == NULL)
    return 1;
  return resource->contract_id != NULL && resource->contract_id[0] != '\0' &&
         resource->contract_version != 0u &&
         cmeta_interface_desc_valid(resource->expected_interface);
}

static inline int
turbo_flow_provider_contract_valid(
    const turbo_flow_provider_contract_v1_t *contract) {
  return contract != NULL && contract->size == sizeof(*contract) &&
         turbo_flow_provider_config_contract_valid(&contract->config) &&
         turbo_flow_provider_resource_requirement_valid(&contract->resource);
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_H */
