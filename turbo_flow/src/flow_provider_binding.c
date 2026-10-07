#include "turbo_flow_provider_binding.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_provider_binding_s {
  char *provider_identity;
  char *component_identity;
  char *module_identity;

  const salts_component_plugin_scope *scope;
  uint64_t component_generation_id;

  turbo_flow_provider_factory factory;
  turbo_flow_provider_contract_v1_t contract;
};

static int provider_component_status(salts_component_plugin_status status) {
  switch (status) {
    case SALTS_COMPONENT_PLUGIN_OK:
      return SALTS_OK;
    case SALTS_COMPONENT_PLUGIN_INVALID_ARGUMENT:
      return SALTS_EINVAL;
    case SALTS_COMPONENT_PLUGIN_CAPACITY_EXCEEDED:
      return SALTS_ENOSPC;
    case SALTS_COMPONENT_PLUGIN_BUSY:
      return SALTS_EBUSY;
    case SALTS_COMPONENT_PLUGIN_COMPONENT_ERROR:
      return SALTS_ENOENT;
    case SALTS_COMPONENT_PLUGIN_PLUGIN_ERROR:
    case SALTS_COMPONENT_PLUGIN_PROVIDER_ERROR:
    case SALTS_COMPONENT_PLUGIN_INVALID_STATE:
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
         candidate->component_identity &&
         candidate->component_identity[0] != '\0';
}

static int provider_binding_live(
    const turbo_flow_provider_binding_t *binding) {
  return binding &&
         binding->scope &&
         binding->component_generation_id != UINT64_C(0) &&
         salts_component_plugin_scope_generation_id(binding->scope) ==
             binding->component_generation_id &&
         turbo_flow_provider_factory_valid(&binding->factory) &&
         turbo_flow_provider_contract_valid(&binding->contract);
}

static void provider_binding_free(turbo_flow_provider_binding_t *binding) {
  if (!binding) return;
  free(binding->provider_identity);
  free(binding->component_identity);
  free(binding->module_identity);
  memset(binding, 0, sizeof(*binding));
  free(binding);
}

int turbo_flow_provider_binding_acquire(
    const salts_component_plugin_scope *component_scope,
    const turbo_flow_provider_resolver_v1_t *resolver,
    const char *provider_identity,
    turbo_flow_provider_binding_t **out,
    turbo_flow_config_error_t *error) {
  turbo_flow_provider_candidate_v1_t candidate =
      TURBO_FLOW_PROVIDER_CANDIDATE_V1_INIT;
  turbo_flow_provider_binding_t *binding = NULL;
  salts_component_service service;
  salts_component_plugin_status component_status;
  cmeta_status projection_status;
  uint64_t generation_id;
  int status;

  if (out) *out = NULL;
  generation_id =
      salts_component_plugin_scope_generation_id(component_scope);
  if (!component_scope || generation_id == UINT64_C(0) ||
      !resolver || resolver->size != sizeof(*resolver) || !resolver->resolve ||
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
        "deployment provider resolver returned an invalid component identity");

  binding = (turbo_flow_provider_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding)
    return provider_error(error, SALTS_ENOMEM, provider_identity,
                          "provider binding allocation failed");

  binding->provider_identity = provider_copy_text(provider_identity);
  binding->component_identity =
      provider_copy_text(candidate.component_identity);
  binding->module_identity = candidate.module_identity
                                 ? provider_copy_text(candidate.module_identity)
                                 : NULL;
  binding->scope = component_scope;
  binding->component_generation_id = generation_id;
  binding->contract =
      (turbo_flow_provider_contract_v1_t)TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;

  if (!binding->provider_identity || !binding->component_identity ||
      (candidate.module_identity && !binding->module_identity)) {
    provider_binding_free(binding);
    return provider_error(error, SALTS_ENOMEM, provider_identity,
                          "provider identity allocation failed");
  }

  memset(&service, 0, sizeof(service));
  component_status = salts_component_plugin_scope_find_service_from(
      component_scope,
      binding->component_identity,
      turbo_flow_provider_factory_interface(),
      &service);
  if (component_status != SALTS_COMPONENT_PLUGIN_OK) {
    status = provider_component_status(component_status);
    provider_binding_free(binding);
    return provider_error(
        error, status, provider_identity,
        "provider Component service was not found in the pinned generation");
  }

  binding->factory = turbo_flow_provider_factory_bind(NULL, NULL);
  projection_status = turbo_flow_provider_factory_borrow_from_object(
      service.object, service.interfaces, &binding->factory);
  if (projection_status != CMETA_OK ||
      !turbo_flow_provider_factory_valid(&binding->factory)) {
    provider_binding_free(binding);
    return provider_error(
        error, SALTS_EPROTO, provider_identity,
        "provider factory Interface projection is incompatible");
  }

  status = turbo_flow_provider_factory_contract(
      &binding->factory, &binding->contract);
  if (status != SALTS_OK) {
    provider_binding_free(binding);
    return provider_error(
        error, status, provider_identity,
        "provider factory contract query failed");
  }
  if (!turbo_flow_provider_contract_valid(&binding->contract)) {
    provider_binding_free(binding);
    return provider_error(
        error, SALTS_EPROTO, provider_identity,
        "provider factory returned an invalid typed contract");
  }

  /* Fail closed if publication changed while control-plane binding ran. */
  if (!provider_binding_live(binding)) {
    provider_binding_free(binding);
    return provider_error(
        error, SALTS_EBUSY, provider_identity,
        "provider Component generation changed during binding");
  }

  *out = binding;
  return SALTS_OK;
}

int turbo_flow_provider_binding_contract(
    const turbo_flow_provider_binding_t *binding,
    turbo_flow_provider_contract_v1_t *out) {
  if (!out || out->size != sizeof(*out) || !provider_binding_live(binding))
    return SALTS_EINVAL;
  *out = binding->contract;
  return SALTS_OK;
}

int turbo_flow_provider_binding_preflight(
    turbo_flow_provider_binding_t *binding,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  if (!provider_binding_live(binding) ||
      !instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0])
    return provider_error(
        error, SALTS_EINVAL,
        binding ? binding->provider_identity : NULL,
        "invalid provider preflight arguments");

  return turbo_flow_provider_factory_preflight(
      &binding->factory, instance, error);
}

int turbo_flow_provider_binding_materialize(
    turbo_flow_provider_binding_t *binding,
    turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  if (!provider_binding_live(binding) ||
      !flow || !instance || instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0] ||
      !owner_out)
    return provider_error(
        error, SALTS_EINVAL,
        binding ? binding->provider_identity : NULL,
        "invalid provider materialization arguments");

  return turbo_flow_provider_factory_materialize(
      &binding->factory, flow, instance, owner_out, error);
}

int turbo_flow_provider_binding_release(
    turbo_flow_provider_binding_t **binding_io) {
  if (!binding_io || !*binding_io) return SALTS_EINVAL;
  provider_binding_free(*binding_io);
  *binding_io = NULL;
  return SALTS_OK;
}
