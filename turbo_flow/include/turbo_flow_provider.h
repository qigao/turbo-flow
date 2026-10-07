#ifndef TURBO_FLOW_PROVIDER_H
#define TURBO_FLOW_PROVIDER_H

#include "turbo_flow.h"
#include "turbo_flow_provider_adapter.h"
#include "turbo_flow_diagnostic.h"
#include "turbo_flow_export.h"

#include <cmeta/interface.h>
#include <cmeta/object_interface.h>
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
 * Both pointers are provider-module owned and may only be used while the
 * enclosing Salts Component generation scope remains live.
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

/*
 * Provider factories are projected from one Salts Component generation.
 * The returned Interface is borrowed from the enclosing Component scope.
 */
CMETA_OBJECT_INTERFACE_ADAPTER(turbo_flow_provider_factory);

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
