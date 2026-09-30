#include "turbo_flow_resource.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_resource_binding_s {
  char *identity;
  char *export_id;
  salts_plugin_registry *registry;
  salts_plugin_lease lease;
  const cmeta_interface_desc *interface_desc;
  void *interface_value;
};

static int resource_plugin_status(salts_plugin_status status) {
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
      return SALTS_EALREADY;
    case SALTS_PLUGIN_BUSY:
      return SALTS_EBUSY;
    case SALTS_PLUGIN_UNSUPPORTED_ABI:
    case SALTS_PLUGIN_INVALID_MANIFEST:
    case SALTS_PLUGIN_INCOMPATIBLE_CONTRACT:
    case SALTS_PLUGIN_QUERY_MISSING:
    case SALTS_PLUGIN_QUERY_REJECTED:
    case SALTS_PLUGIN_INVALID_STATE:
      return SALTS_EPROTO;
    case SALTS_PLUGIN_LOAD_FAILED:
    case SALTS_PLUGIN_UNLOAD_FAILED:
      return SALTS_EIO;
    case SALTS_PLUGIN_DUPLICATE_PLUGIN_ID:
    case SALTS_PLUGIN_DUPLICATE_EXPORT:
      return SALTS_EALREADY;
  }
  return SALTS_EPROTO;
}

static int resource_error(turbo_flow_config_error_t *error, int status,
                          const char *resource_name, const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.resources.%s",
                   resource_name ? resource_name : "");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "resource binding failed");
  }
  return status;
}

static char *resource_copy_text(const char *text) {
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

static int resource_requirement_present(
    const turbo_flow_provider_resource_requirement_v1_t *requirement) {
  return requirement &&
         requirement->size == sizeof(*requirement) &&
         requirement->contract_id && requirement->contract_id[0] &&
         requirement->contract_version != 0u &&
         cmeta_interface_desc_valid(requirement->expected_interface);
}

static int resource_candidate_valid(
    const turbo_flow_resource_candidate_v1_t *candidate) {
  return candidate &&
         candidate->size == sizeof(*candidate) &&
         candidate->identity && candidate->identity[0] &&
         candidate->registry &&
         salts_plugin_ref_valid(candidate->plugin) &&
         candidate->export_id && candidate->export_id[0];
}

static int resource_binding_cleanup_failed_acquire(
    turbo_flow_resource_binding_t *binding,
    turbo_flow_resource_binding_t **out,
    turbo_flow_config_error_t *error,
    const char *resource_name,
    int original_status,
    const char *message) {
  salts_plugin_status released;
  if (!binding || !salts_plugin_lease_valid(binding->lease)) {
    free(binding ? binding->identity : NULL);
    free(binding ? binding->export_id : NULL);
    free(binding);
    return resource_error(error, original_status, resource_name, message);
  }

  released = salts_plugin_registry_release(binding->registry, &binding->lease);
  if (released != SALTS_PLUGIN_OK) {
    *out = binding;
    return resource_error(error, resource_plugin_status(released), resource_name,
                          "resource lease cleanup failed after binding rejection");
  }

  free(binding->identity);
  free(binding->export_id);
  free(binding);
  return resource_error(error, original_status, resource_name, message);
}

int turbo_flow_resource_binding_acquire(
    const turbo_flow_resource_resolver_v1_t *resolver,
    const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_binding_t **out,
    turbo_flow_config_error_t *error) {
  turbo_flow_resource_candidate_v1_t candidate =
      TURBO_FLOW_RESOURCE_CANDIDATE_V1_INIT;
  turbo_flow_resource_binding_t *binding = NULL;
  const salts_plugin_manifest *manifest = NULL;
  const salts_plugin_export *entry = NULL;
  salts_plugin_status plugin_status;
  int status;

  if (out) *out = NULL;
  if (!resolver || resolver->size != sizeof(*resolver) || !resolver->resolve ||
      !resource_name || !resource_name[0] || !out ||
      !resource_requirement_present(requirement))
    return resource_error(error, SALTS_EINVAL, resource_name,
                          "invalid resource binding arguments");

  if (error && error->size == sizeof(*error))
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;

  status = resolver->resolve(resolver->ctx, resource_name, requirement,
                             &candidate, error);
  if (status != SALTS_OK) {
    if (!error || error->size != sizeof(*error) || error->status == SALTS_OK)
      resource_error(error, status, resource_name,
                     "deployment resource resolver rejected the resource");
    return status;
  }
  if (!resource_candidate_valid(&candidate))
    return resource_error(error, SALTS_EPROTO, resource_name,
                          "deployment resource resolver returned an invalid candidate");

  binding = (turbo_flow_resource_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding)
    return resource_error(error, SALTS_ENOMEM, resource_name,
                          "resource binding allocation failed");
  binding->identity = resource_copy_text(candidate.identity);
  binding->export_id = resource_copy_text(candidate.export_id);
  binding->registry = candidate.registry;
  if (!binding->identity || !binding->export_id) {
    free(binding->identity);
    free(binding->export_id);
    free(binding);
    return resource_error(error, SALTS_ENOMEM, resource_name,
                          "resource identity allocation failed");
  }

  plugin_status = salts_plugin_registry_acquire(
      binding->registry, candidate.plugin, &binding->lease, &manifest);
  if (plugin_status != SALTS_PLUGIN_OK) {
    status = resource_plugin_status(plugin_status);
    free(binding->identity);
    free(binding->export_id);
    free(binding);
    return resource_error(error, status, resource_name,
                          "resource plugin lease acquisition failed");
  }

  plugin_status = salts_plugin_manifest_find_export(
      manifest, binding->export_id, &entry);
  if (plugin_status != SALTS_PLUGIN_OK) {
    status = resource_plugin_status(plugin_status);
    return resource_binding_cleanup_failed_acquire(
        binding, out, error, resource_name, status,
        "resource Interface export was not found");
  }

  plugin_status = salts_plugin_export_require_interface(
      entry, requirement->contract_id, requirement->contract_version,
      requirement->required_capabilities, requirement->expected_interface);
  if (plugin_status != SALTS_PLUGIN_OK) {
    status = resource_plugin_status(plugin_status);
    return resource_binding_cleanup_failed_acquire(
        binding, out, error, resource_name, status,
        "resource Interface contract is incompatible");
  }

  binding->interface_desc = entry->value.interface.desc;
  binding->interface_value = entry->value.interface.value;
  *out = binding;
  return SALTS_OK;
}

int turbo_flow_resource_binding_view(
    const turbo_flow_resource_binding_t *binding,
    turbo_flow_provider_resource_view_v1_t *out) {
  if (!binding || !out || out->size != sizeof(*out) ||
      !binding->identity || !binding->export_id ||
      !salts_plugin_lease_valid(binding->lease) ||
      !cmeta_interface_desc_valid(binding->interface_desc) ||
      !binding->interface_value)
    return SALTS_EINVAL;

  *out = (turbo_flow_provider_resource_view_v1_t)
      TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
  out->identity = binding->identity;
  out->export_id = binding->export_id;
  out->interface_desc = binding->interface_desc;
  out->interface_value = binding->interface_value;
  return SALTS_OK;
}

int turbo_flow_resource_binding_release(
    turbo_flow_resource_binding_t **binding_io) {
  turbo_flow_resource_binding_t *binding;
  salts_plugin_status status;

  if (!binding_io || !*binding_io) return SALTS_EINVAL;
  binding = *binding_io;
  if (!binding->registry || !salts_plugin_lease_valid(binding->lease))
    return SALTS_EINVAL;

  status = salts_plugin_registry_release(binding->registry, &binding->lease);
  if (status != SALTS_PLUGIN_OK) return resource_plugin_status(status);

  free(binding->identity);
  free(binding->export_id);
  free(binding);
  *binding_io = NULL;
  return SALTS_OK;
}
