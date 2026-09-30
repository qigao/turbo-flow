#include "turbo_flow_provider_binding.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_provider_binding_s {
  char *provider_identity;
  char *module_identity;
  salts_plugin_registry *registry;
  salts_plugin_lease lease;
  turbo_flow_provider_factory *factory;
  turbo_flow_provider_contract_v1_t contract;
};

static int provider_plugin_status(salts_plugin_status status) {
  switch (status) {
    case SALTS_PLUGIN_OK:
      return SALTS_OK;
    case SALTS_PLUGIN_INVALID_ARGUMENT:
      return SALTS_EINVAL;
    case SALTS_PLUGIN_UNKNOWN_EXPORT:
    case SALTS_PLUGIN_UNKNOWN_PLUGIN:
    case SALTS_PLUGIN_STALE:
      return SALTS_ENOENT;
    case SALTS_PLUGIN_CAPACITY_EXCEEDED:
      return SALTS_ENOSPC;
    case SALTS_PLUGIN_ALLOCATION_FAILED:
      return SALTS_ENOMEM;
    case SALTS_PLUGIN_ALREADY:
    case SALTS_PLUGIN_DUPLICATE_PLUGIN_ID:
    case SALTS_PLUGIN_DUPLICATE_EXPORT:
      return SALTS_EALREADY;
    case SALTS_PLUGIN_BUSY:
      return SALTS_EBUSY;
    case SALTS_PLUGIN_LOAD_FAILED:
    case SALTS_PLUGIN_UNLOAD_FAILED:
      return SALTS_EIO;
    case SALTS_PLUGIN_UNSUPPORTED_ABI:
    case SALTS_PLUGIN_INVALID_MANIFEST:
    case SALTS_PLUGIN_INCOMPATIBLE_CONTRACT:
    case SALTS_PLUGIN_QUERY_MISSING:
    case SALTS_PLUGIN_QUERY_REJECTED:
    case SALTS_PLUGIN_INVALID_STATE:
      return SALTS_EPROTO;
  }
  return SALTS_EPROTO;
}

static int provider_error(turbo_flow_config_error_t *error, int status,
                          const char *provider_identity,
                          const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.providers.%s",
                   provider_identity ? provider_identity : "");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "provider binding failed");
  }
  return status;
}

static char *provider_copy_text(const char *text) {
  size_t size;
  char *copy;
  if (!text || !text[0]) return NULL;
  size = strlen(text);
  if (size == SIZE_MAX) return NULL;
  copy = (char *)malloc(size + 1u);
  if (!copy) return NULL;
  memcpy(copy, text, size + 1u);
  return copy;
}

static int provider_candidate_valid(
    const turbo_flow_provider_candidate_v1_t *candidate) {
  return candidate &&
         candidate->size == sizeof(*candidate) &&
         candidate->registry &&
         salts_plugin_ref_valid(candidate->plugin);
}

static void provider_binding_free(turbo_flow_provider_binding_t *binding) {
  if (!binding) return;
  free(binding->provider_identity);
  free(binding->module_identity);
  free(binding);
}

static int provider_cleanup_failed_acquire(
    turbo_flow_provider_binding_t *binding,
    turbo_flow_provider_binding_t **out,
    turbo_flow_config_error_t *error,
    const char *provider_identity,
    int original_status,
    const char *message) {
  salts_plugin_status released;
  if (!binding || !salts_plugin_lease_valid(binding->lease)) {
    provider_binding_free(binding);
    return provider_error(error, original_status, provider_identity, message);
  }

  released = salts_plugin_registry_release(binding->registry, &binding->lease);
  if (released != SALTS_PLUGIN_OK) {
    *out = binding;
    return provider_error(
        error, provider_plugin_status(released), provider_identity,
        "provider lease cleanup failed after binding rejection");
  }

  provider_binding_free(binding);
  return provider_error(error, original_status, provider_identity, message);
}

int turbo_flow_provider_binding_acquire(
    const turbo_flow_provider_resolver_v1_t *resolver,
    const char *provider_identity,
    turbo_flow_provider_binding_t **out,
    turbo_flow_config_error_t *error) {
  turbo_flow_provider_candidate_v1_t candidate =
      TURBO_FLOW_PROVIDER_CANDIDATE_V1_INIT;
  turbo_flow_provider_binding_t *binding = NULL;
  const salts_plugin_manifest *manifest = NULL;
  const salts_plugin_export *entry = NULL;
  salts_plugin_status plugin_status;
  int status;

  if (out) *out = NULL;
  if (!resolver || resolver->size != sizeof(*resolver) || !resolver->resolve ||
      !provider_identity || !provider_identity[0] || !out)
    return provider_error(error, SALTS_EINVAL, provider_identity,
                          "invalid provider binding arguments");

  if (error && error->size == sizeof(*error))
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;

  status = resolver->resolve(
      resolver->ctx, provider_identity, &candidate, error);
  if (status != SALTS_OK) {
    if (!error || error->size != sizeof(*error) || error->status == SALTS_OK)
      provider_error(error, status, provider_identity,
                     "deployment provider resolver rejected the provider");
    return status;
  }
  if (!provider_candidate_valid(&candidate))
    return provider_error(
        error, SALTS_EPROTO, provider_identity,
        "deployment provider resolver returned an invalid candidate");

  binding = (turbo_flow_provider_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding)
    return provider_error(error, SALTS_ENOMEM, provider_identity,
                          "provider binding allocation failed");

  binding->provider_identity = provider_copy_text(provider_identity);
  binding->module_identity = candidate.module_identity
                                 ? provider_copy_text(candidate.module_identity)
                                 : NULL;
  binding->registry = candidate.registry;
  binding->contract =
      (turbo_flow_provider_contract_v1_t)TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  if (!binding->provider_identity ||
      (candidate.module_identity && !binding->module_identity)) {
    provider_binding_free(binding);
    return provider_error(error, SALTS_ENOMEM, provider_identity,
                          "provider identity allocation failed");
  }

  plugin_status = salts_plugin_registry_acquire(
      binding->registry, candidate.plugin, &binding->lease, &manifest);
  if (plugin_status != SALTS_PLUGIN_OK) {
    status = provider_plugin_status(plugin_status);
    provider_binding_free(binding);
    return provider_error(error, status, provider_identity,
                          "provider plugin lease acquisition failed");
  }

  plugin_status = salts_plugin_manifest_find_export(
      manifest, binding->provider_identity, &entry);
  if (plugin_status != SALTS_PLUGIN_OK) {
    status = provider_plugin_status(plugin_status);
    return provider_cleanup_failed_acquire(
        binding, out, error, provider_identity, status,
        "provider factory export was not found");
  }

  plugin_status = salts_plugin_export_require_interface(
      entry, TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
      TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION, 0u,
      turbo_flow_provider_factory_interface());
  if (plugin_status != SALTS_PLUGIN_OK) {
    status = provider_plugin_status(plugin_status);
    return provider_cleanup_failed_acquire(
        binding, out, error, provider_identity, status,
        "provider factory Interface contract is incompatible");
  }

  binding->factory =
      (turbo_flow_provider_factory *)entry->value.interface.value;
  if (!turbo_flow_provider_factory_valid(binding->factory)) {
    return provider_cleanup_failed_acquire(
        binding, out, error, provider_identity, SALTS_EPROTO,
        "provider factory Interface handle is invalid");
  }

  status = turbo_flow_provider_factory_contract(
      binding->factory, &binding->contract);
  if (status != SALTS_OK) {
    return provider_cleanup_failed_acquire(
        binding, out, error, provider_identity, status,
        "provider factory contract query failed");
  }
  if (!turbo_flow_provider_contract_valid(&binding->contract)) {
    return provider_cleanup_failed_acquire(
        binding, out, error, provider_identity, SALTS_EPROTO,
        "provider factory returned an invalid typed contract");
  }

  *out = binding;
  return SALTS_OK;
}

int turbo_flow_provider_binding_contract(
    const turbo_flow_provider_binding_t *binding,
    turbo_flow_provider_contract_v1_t *out) {
  if (!binding || !out || out->size != sizeof(*out) ||
      !salts_plugin_lease_valid(binding->lease) ||
      !binding->factory ||
      !turbo_flow_provider_contract_valid(&binding->contract))
    return SALTS_EINVAL;
  *out = binding->contract;
  return SALTS_OK;
}

int turbo_flow_provider_binding_preflight(
    turbo_flow_provider_binding_t *binding,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  if (!binding || !salts_plugin_lease_valid(binding->lease) ||
      !turbo_flow_provider_factory_valid(binding->factory) ||
      !instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0])
    return provider_error(
        error, SALTS_EINVAL,
        binding ? binding->provider_identity : NULL,
        "invalid provider preflight arguments");

  return turbo_flow_provider_factory_preflight(
      binding->factory, instance, error);
}

int turbo_flow_provider_binding_materialize(
    turbo_flow_provider_binding_t *binding,
    turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  if (!binding || !salts_plugin_lease_valid(binding->lease) ||
      !turbo_flow_provider_factory_valid(binding->factory) ||
      !flow || !instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0] ||
      !owner_out)
    return provider_error(
        error, SALTS_EINVAL,
        binding ? binding->provider_identity : NULL,
        "invalid provider materialization arguments");

  return turbo_flow_provider_factory_materialize(
      binding->factory, flow, instance, owner_out, error);
}

int turbo_flow_provider_binding_release(
    turbo_flow_provider_binding_t **binding_io) {
  turbo_flow_provider_binding_t *binding;
  salts_plugin_status status;

  if (!binding_io || !*binding_io) return SALTS_EINVAL;
  binding = *binding_io;
  if (!binding->registry || !salts_plugin_lease_valid(binding->lease))
    return SALTS_EINVAL;

  status = salts_plugin_registry_release(binding->registry, &binding->lease);
  if (status != SALTS_PLUGIN_OK) return provider_plugin_status(status);

  provider_binding_free(binding);
  *binding_io = NULL;
  return SALTS_OK;
}
