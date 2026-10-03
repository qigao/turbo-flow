#include "flow_plugin_operation_internal.h"
#include "flow_projection_owner_internal.h"
#include "flow_provider_generation_internal.h"
#include "flow_internal.h"
#include "turbo_flow_plugin_generation.h"

#include "turbo_flow_stl_error_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FLOW_PLUGIN_MATERIALIZER_MAX_RETAINED_BYTES = 67108864u };

typedef struct flow_plugin_materializer_projection_context_s {
  size_t native_bytes;
} flow_plugin_materializer_projection_context_t;

static int flow_plugin_generation_materializer_error(
    turbo_flow_config_error_t *error, int status, size_t index,
    const char *field, const char *message);

struct turbo_flow_plugin_materializer_binding_s {
  turbo_flow_plugin_materializer_v1_t materializer;
  turbo_flow_projection_owner_t *owner;
  size_t config_index;
};

struct turbo_flow_plugin_generation_s {
  turbo_flow_t *flow;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  flow_provider_generation_t *providers;
  vec_t bindings;
  vec_t materializers;
  turbo_flow_plugin_result_domain_t *domain;
  turbo_flow_config_error_t cleanup_error;
  int failed;
  size_t leases;
  int poll_closed;
  turbo_flow_plugin_generation_state_t state;
};

static int flow_plugin_generation_error(turbo_flow_config_error_t *error, int status,
                                        const char *path, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s", path ? path : "$.generation");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int flow_plugin_materializer_projection_clone(const void *value, void *ctx,
                                                     void **out) {
  const flow_plugin_materializer_projection_context_t *projection =
      (const flow_plugin_materializer_projection_context_t *)ctx;
  void *copy;
  if (out) *out = NULL;
  if (!value || !projection || !projection->native_bytes || !out) return SALTS_EINVAL;
  copy = malloc(projection->native_bytes);
  if (!copy) return SALTS_ENOMEM;
  memcpy(copy, value, projection->native_bytes);
  *out = copy;
  return SALTS_OK;
}

static void flow_plugin_materializer_projection_destroy(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static int flow_plugin_materializer_projection_release(void *ctx) {
  free(ctx);
  return SALTS_OK;
}

static int flow_plugin_materializer_owner_discard(
    turbo_flow_plugin_materializer_binding_t *binding) {
  int rc;
  if (!binding || !binding->owner) return SALTS_OK;
  rc = turbo_flow_projection_owner_stop(binding->owner);
  if (rc == SALTS_OK) rc = turbo_flow_projection_owner_destroy(binding->owner);
  if (rc == SALTS_OK) binding->owner = NULL;
  return rc;
}

static int flow_plugin_materializers_discard(vec_t *bindings) {
  int first = SALTS_OK;
  if (!bindings) return SALTS_OK;
  for (size_t i = vec_size(bindings); i > 0u; --i) {
    turbo_flow_plugin_materializer_binding_t *binding =
        (turbo_flow_plugin_materializer_binding_t *)vec_at(bindings, i - 1u);
    int rc = flow_plugin_materializer_owner_discard(binding);
    if (first == SALTS_OK && rc != SALTS_OK) first = rc;
  }
  vec_destroy(bindings);
  return first;
}

static int flow_plugin_materializers_stop_and_check(
    vec_t *bindings, turbo_flow_config_error_t *error) {
  if (!bindings) return SALTS_OK;
  for (size_t i = 0u; i < vec_size(bindings); ++i) {
    turbo_flow_plugin_materializer_binding_t *binding =
        (turbo_flow_plugin_materializer_binding_t *)vec_at(bindings, i);
    turbo_flow_projection_owner_snapshot_t state =
        TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    int rc;
    if (!binding || !binding->owner) continue;
    rc = turbo_flow_projection_owner_stop(binding->owner);
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, binding->config_index, NULL,
          "failed to stop materializer projection admission");
    rc = turbo_flow_projection_owner_snapshot(binding->owner, &state);
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, binding->config_index, NULL,
          "failed to inspect materializer projection admission");
    if (state.outstanding != 0u)
      return flow_plugin_generation_materializer_error(
          error, SALTS_EBUSY, binding->config_index, NULL,
          "materialized projections are still outstanding");
  }
  return SALTS_OK;
}

static int flow_plugin_generation_materializer_error(
    turbo_flow_config_error_t *error, int status, size_t index,
    const char *field, const char *message) {
  char path[TURBO_FLOW_DIAGNOSTIC_PATH_MAX + 1u];
  (void)snprintf(path, sizeof(path), "$.materializer_bindings[%zu]%s%s", index,
                 field ? "." : "", field ? field : "");
  return flow_plugin_generation_error(error, status, path, message);
}

static int flow_plugin_generation_materializer_encoding(
    turbo_flow_config_materializer_encoding_t configured,
    turbo_flow_data_encoding_t *out) {
  if (!out) return SALTS_EINVAL;
  switch (configured) {
  case TURBO_FLOW_CONFIG_MATERIALIZER_TBE:
    *out = TURBO_FLOW_DATA_ENCODING_TBE;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_JSON:
    *out = TURBO_FLOW_DATA_ENCODING_JSON;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_CSV:
    *out = TURBO_FLOW_DATA_ENCODING_CSV;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_XML:
    *out = TURBO_FLOW_DATA_ENCODING_XML;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_UTF8:
    *out = TURBO_FLOW_DATA_ENCODING_UTF8;
    return SALTS_OK;
  case TURBO_FLOW_CONFIG_MATERIALIZER_OPAQUE:
    *out = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    return SALTS_OK;
  default:
    return SALTS_EINVAL;
  }
}

static int flow_plugin_generation_prepare_materializers(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    const turbo_flow_resolved_config_t *resolved,
    const vec_t *operations, vec_t *out,
    turbo_flow_config_error_t *error) {
  turbo_flow_plugin_materializer_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
  size_t count = 0u;
  int rc;
  rc = turbo_flow_resolved_config_materializer_binding_count(resolved, &count);
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer binding config is unavailable");
  rc = turbo_flow_stl_error(
      vec_init_bytes(out, sizeof(turbo_flow_plugin_materializer_binding_t),
                     _Alignof(turbo_flow_max_align_t), count));
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer binding allocation failed");
  if (count == 0u) return SALTS_OK;
  rc = turbo_flow_stl_error(vec_reserve(out, count));
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer binding reservation failed");
  rc = turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog);
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, 0u, NULL, "materializer catalog is unavailable");

  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resolved_materializer_binding_view_t wanted =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    const turbo_flow_plugin_materializer_catalog_entry_v1_t *selected = NULL;
    turbo_flow_plugin_materializer_binding_t compiled;
    turbo_flow_projection_owner_config_t owner_config =
        TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
    flow_plugin_materializer_projection_context_t *projection_ctx = NULL;
    turbo_flow_data_encoding_t encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    size_t consumer_count = 0u;
    size_t projection_capacity = 0u;
    size_t max_retained_bytes = 0u;
    rc = turbo_flow_resolved_config_materializer_binding_at(resolved, i, &wanted);
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, i, NULL, "materializer binding projection failed");
    rc = flow_plugin_generation_materializer_encoding(wanted.encoding, &encoding);
    if (rc != SALTS_OK)
      return flow_plugin_generation_materializer_error(
          error, rc, i, "encoding", "materializer encoding is invalid");

    for (size_t j = 0u; j < catalog.count; ++j) {
      const turbo_flow_plugin_materializer_catalog_entry_v1_t *entry = &catalog.entries[j];
      if (!entry->plugin_id || strcmp(entry->plugin_id, wanted.plugin) != 0)
        continue;
      if (strcmp(entry->materializer.schema.schema_name, wanted.schema) != 0 ||
          entry->materializer.schema.schema_version != wanted.schema_version ||
          entry->materializer.schema.encoding != encoding)
        continue;
      selected = entry;
      break;
    }
    if (!selected)
      return flow_plugin_generation_materializer_error(
          error, SALTS_ENOENT, i, "plugin",
          "configured materializer provider/schema/version/encoding was not registered");

    for (size_t j = 0u; j < vec_size(operations); ++j) {
      const flow_plugin_operation_binding_t *operation =
          (const flow_plugin_operation_binding_t *)vec_at_const(operations, j);
      if (!operation || !operation->operation.input.data ||
          !operation->operation.input.projection)
        continue;
      if (strcmp(operation->operation.input.data->stable_id, wanted.schema) != 0 ||
          operation->operation.input.schema_version != wanted.schema_version ||
          operation->operation.input.projection->encoding != encoding)
        continue;
      rc = turbo_flow_data_schema_match(
          &selected->materializer.schema, selected->materializer.data,
          operation->operation.input.projection, operation->operation.input.data);
      if (rc != SALTS_OK)
        return flow_plugin_generation_materializer_error(
            error, SALTS_EPROTO, i, "schema",
            "materializer schema/CMeta does not exactly match every operation input consumer");
      if (operation->request.limits.max_inflight > SIZE_MAX - projection_capacity)
        return flow_plugin_generation_materializer_error(
            error, SALTS_EINVAL, i, "schema",
            "materializer projection capacity overflow");
      projection_capacity += operation->request.limits.max_inflight;
      ++consumer_count;
    }
    if (consumer_count == 0u)
      return flow_plugin_generation_materializer_error(
          error, SALTS_EPROTO, i, "schema",
          "materializer binding has no exact typed-operation input consumer");

    if (!projection_capacity ||
        selected->materializer.native_bytes > SIZE_MAX / projection_capacity)
      return flow_plugin_generation_materializer_error(
          error, SALTS_EINVAL, i, "schema",
          "materializer retained-byte capacity overflow");
    max_retained_bytes = projection_capacity * selected->materializer.native_bytes;
    if (max_retained_bytes > FLOW_PLUGIN_MATERIALIZER_MAX_RETAINED_BYTES)
      return flow_plugin_generation_materializer_error(
          error, SALTS_ENOSPC, i, "schema",
          "materializer retained native-byte budget exceeds 64 MiB");
    projection_ctx =
        (flow_plugin_materializer_projection_context_t *)calloc(1u, sizeof(*projection_ctx));
    if (!projection_ctx)
      return flow_plugin_generation_materializer_error(
          error, SALTS_ENOMEM, i, NULL,
          "materializer projection context allocation failed");
    projection_ctx->native_bytes = selected->materializer.native_bytes;
    owner_config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE |
                         TURBO_FLOW_PROJECTION_CROSS_THREAD |
                         TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
    owner_config.capacity = projection_capacity;
    owner_config.max_result_bytes = selected->materializer.native_bytes;
    owner_config.max_retained_bytes = max_retained_bytes;
    owner_config.schema = &selected->materializer.schema;
    owner_config.clone = flow_plugin_materializer_projection_clone;
    owner_config.destroy = flow_plugin_materializer_projection_destroy;
    owner_config.ctx = projection_ctx;
    owner_config.release_context = flow_plugin_materializer_projection_release;

    memset(&compiled, 0, sizeof(compiled));
    compiled.materializer = selected->materializer;
    compiled.config_index = i;
    rc = turbo_flow_plugin_projection_owner_create(snapshot, &owner_config, &compiled.owner);
    if (rc != SALTS_OK) {
      free(projection_ctx);
      return flow_plugin_generation_materializer_error(
          error, rc, i, NULL, "materializer projection owner creation failed");
    }
    rc = turbo_flow_stl_error(vec_push(out, &compiled));
    if (rc != SALTS_OK) {
      int cleanup_rc = flow_plugin_materializer_owner_discard(&compiled);
      return flow_plugin_generation_materializer_error(
          error, cleanup_rc != SALTS_OK ? cleanup_rc : rc, i, NULL,
          "materializer binding commit failed");
    }
  }
  return SALTS_OK;
}

static int flow_plugin_generation_bind_operation_materializers(
    vec_t *operations, const vec_t *materializers, turbo_flow_config_error_t *error) {
  for (size_t i = 0u; i < vec_size(operations); ++i) {
    flow_plugin_operation_binding_t *operation =
        (flow_plugin_operation_binding_t *)vec_at(operations, i);
    const turbo_flow_plugin_materializer_binding_t *selected = NULL;
    if (!operation || !operation->operation.input.data ||
        !operation->operation.input.projection)
      continue;
    for (size_t j = 0u; j < vec_size(materializers); ++j) {
      const turbo_flow_plugin_materializer_binding_t *candidate =
          (const turbo_flow_plugin_materializer_binding_t *)vec_at_const(materializers, j);
      int rc;
      if (!candidate || !candidate->owner || !candidate->materializer.data)
        continue;
      if (strcmp(candidate->materializer.schema.schema_name,
                 operation->operation.input.data->stable_id) != 0 ||
          candidate->materializer.schema.schema_version !=
              operation->operation.input.schema_version ||
          candidate->materializer.schema.encoding !=
              operation->operation.input.projection->encoding)
        continue;
      rc = turbo_flow_data_schema_match(
          &candidate->materializer.schema, candidate->materializer.data,
          operation->operation.input.projection, operation->operation.input.data);
      if (rc != SALTS_OK)
        return flow_plugin_generation_error(
            error, SALTS_EPROTO, "$.materializer_bindings",
            "materializer schema does not exactly match typed-operation input");
      if (selected)
        return flow_plugin_generation_error(
            error, SALTS_EPROTO, "$.materializer_bindings",
            "multiple compiled materializers match one typed-operation input");
      selected = candidate;
    }
    operation->materializer = selected;
  }
  return SALTS_OK;
}

int turbo_flow_plugin_generation_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    const turbo_flow_resolved_config_t *resolved,
    turbo_flow_t **flow_io,
    const turbo_flow_plugin_generation_config_t *config,
    turbo_flow_plugin_result_domain_t *domain,
    turbo_flow_plugin_generation_t **generation_out,
    turbo_flow_plugin_generation_t **cleanup_out,
    turbo_flow_config_error_t *error) {
  turbo_flow_plugin_generation_t *generation;
  turbo_flow_config_error_t cleanup_error =
      TURBO_FLOW_CONFIG_ERROR_INIT;
  int rc;

  if (generation_out) *generation_out = NULL;
  if (cleanup_out) *cleanup_out = NULL;
  if (!snapshot || !resolved || !flow_io || !*flow_io || !config ||
      !generation_out || !cleanup_out || generation_out == cleanup_out ||
      !error || error->size != sizeof(*error) ||
      config->size != sizeof(*config) ||
      config->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      config->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      config->owner_capacity > TURBO_FLOW_PLUGIN_GENERATION_MAX_OWNERS)
    return flow_plugin_generation_error(
        error, SALTS_EINVAL, "$.generation",
        "invalid generation arguments");

  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (turbo_flow_state(*flow_io) != TURBO_FLOW_STATE_PARSED)
    return flow_plugin_generation_error(
        error, SALTS_EINVAL, "$.graph", "Graph must be parsed");

  generation =
      (turbo_flow_plugin_generation_t *)calloc(1u, sizeof(*generation));
  if (!generation)
    return flow_plugin_generation_error(
        error, SALTS_ENOMEM, "$.generation", "allocation failed");
  generation->cleanup_error =
      (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;

  /*
   * Canonical provider prepare is side-effect free: it retains provider/resource
   * Salts Plugin leases, compiles typed MessagePlan config and completes provider
   * preflight for every materialization root before Graph ownership moves.
   */
  rc = flow_provider_generation_prepare(
      *flow_io, config->provider_resolver, config->resource_resolver,
      config->owner_capacity, &generation->providers, error);
  if (rc != SALTS_OK) goto preflight_failed;

  rc = flow_plugin_operations_prepare(
      snapshot, resolved, *flow_io, domain,
      config->operation_memory_budget_bytes, &generation->bindings, error);
  if (rc != SALTS_OK) goto preflight_failed;

  rc = flow_plugin_generation_prepare_materializers(
      snapshot, resolved, &generation->bindings,
      &generation->materializers, error);
  if (rc != SALTS_OK) goto preflight_failed;

  rc = flow_plugin_generation_bind_operation_materializers(
      &generation->bindings, &generation->materializers, error);
  if (rc != SALTS_OK) goto preflight_failed;

  rc = flow_plugin_operations_preflight(&generation->bindings, error);
  if (rc != SALTS_OK) goto preflight_failed;

  rc = turbo_flow_plugin_catalog_snapshot_retain(snapshot);
  if (rc != SALTS_OK) goto preflight_failed;
  generation->snapshot = snapshot;

  if (domain) {
    rc = flow_plugin_result_domain_attach(domain);
    if (rc != SALTS_OK) goto preflight_failed;
  }
  generation->domain = domain;

  generation->flow = *flow_io;
  *flow_io = NULL;

  rc = flow_provider_generation_materialize(
      generation->providers, generation->flow, error);
  if (rc == SALTS_OK)
    rc = flow_plugin_operations_materialize(
        &generation->bindings, domain, generation->flow, error);

  if (rc == SALTS_OK) {
    rc = turbo_flow_compile(generation->flow);
    if (rc != SALTS_OK) {
      const turbo_flow_error_t *graph_error =
          turbo_flow_last_error(generation->flow);
      flow_plugin_generation_error(
          error, rc, "$.graph",
          graph_error && graph_error->message[0]
              ? graph_error->message
              : "Graph compile failed");
    }
  }

  if (rc != SALTS_OK) {
    generation->failed = 1;
    generation->state = TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP;
    if (turbo_flow_plugin_generation_destroy(
            generation, 0u, &cleanup_error) != SALTS_OK)
      *cleanup_out = generation;
    return rc;
  }

  generation->state = TURBO_FLOW_PLUGIN_GENERATION_COMPILED;
  *generation_out = generation;
  return SALTS_OK;

preflight_failed:
  if (error->status == SALTS_OK)
    flow_plugin_generation_error(
        error, rc, "$.generation.preflight",
        "generation preflight failed");

  if (generation->snapshot) {
    turbo_flow_plugin_catalog_snapshot_destroy(generation->snapshot);
    generation->snapshot = NULL;
  }
  if (generation->domain) {
    flow_plugin_result_domain_detach(generation->domain);
    generation->domain = NULL;
  }
  (void)flow_plugin_materializers_discard(&generation->materializers);
  flow_plugin_operations_free(&generation->bindings);

  if (generation->providers) {
    int cleanup = flow_provider_generation_release(
        &generation->providers, &cleanup_error);
    if (cleanup != SALTS_OK) {
      generation->failed = 1;
      generation->state = TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP;
      generation->cleanup_error = cleanup_error;
      *cleanup_out = generation;
      return rc;
    }
  }

  free(generation);
  return rc;
}

turbo_flow_t *turbo_flow_plugin_generation_flow(turbo_flow_plugin_generation_t *generation) {
  return generation && !generation->failed ? generation->flow : NULL;
}

turbo_flow_plugin_generation_state_t
turbo_flow_plugin_generation_state(const turbo_flow_plugin_generation_t *generation) {
  if (!generation) return TURBO_FLOW_PLUGIN_GENERATION_INVALID;
  if (generation->failed) return TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP;
  if (generation->state == TURBO_FLOW_PLUGIN_GENERATION_COMPILED && generation->flow &&
      turbo_flow_state(generation->flow) == TURBO_FLOW_STATE_STARTED)
    return TURBO_FLOW_PLUGIN_GENERATION_ACTIVE;
  return generation->state;
}

size_t turbo_flow_plugin_generation_owner_count(const turbo_flow_plugin_generation_t *generation) {
  return generation ? vec_size(&generation->owners) : 0u;
}

size_t turbo_flow_plugin_generation_materializer_count(
    const turbo_flow_plugin_generation_t *generation) {
  return generation ? vec_size(&generation->materializers) : 0u;
}

int turbo_flow_plugin_generation_materializer_at(
    const turbo_flow_plugin_generation_t *generation, size_t index,
    const turbo_flow_plugin_materializer_binding_t **out) {
  const turbo_flow_plugin_materializer_binding_t *binding;
  if (out) *out = NULL;
  if (!generation || !out || index >= vec_size(&generation->materializers))
    return SALTS_EINVAL;
  binding = (const turbo_flow_plugin_materializer_binding_t *)
      vec_at_const(&generation->materializers, index);
  if (!binding || !binding->owner || !binding->materializer.materialize)
    return SALTS_EPROTO;
  *out = binding;
  return SALTS_OK;
}

static int flow_plugin_materializer_descriptor_match(
    const turbo_flow_plugin_materializer_binding_t *binding,
    const turbo_flow_content_descriptor_t *descriptor) {
  const turbo_flow_data_schema_t *schema;
  if (!binding || !descriptor ||
      turbo_flow_content_descriptor_check(descriptor) != SALTS_OK)
    return SALTS_EINVAL;
  schema = &binding->materializer.schema;
  if ((descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u)
    return SALTS_EPROTO;
  return descriptor->domain == schema->domain &&
                 descriptor->encoding == schema->encoding &&
                 descriptor->schema_version == schema->schema_version &&
                 strcmp(descriptor->schema_name, schema->schema_name) == 0 &&
                 strcmp(descriptor->type_name, schema->type_name) == 0
             ? SALTS_OK
             : SALTS_EPROTO;
}

int turbo_flow_plugin_materializer_materialize(
    const turbo_flow_plugin_materializer_binding_t *binding, turbo_flow_msg_t *msg,
    turbo_flow_config_error_t *error) {
  turbo_flow_plugin_materializer_input_v1_t input =
      TURBO_FLOW_PLUGIN_MATERIALIZER_INPUT_V1_INIT;
  const turbo_flow_content_descriptor_t *descriptor;
  void *value = NULL;
  int rc;
  if (!error || error->size < sizeof(*error))
    return SALTS_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (!binding || !binding->owner || !binding->materializer.materialize || !msg)
    return flow_plugin_generation_materializer_error(
        error, SALTS_EINVAL, binding ? binding->config_index : 0u, "execute",
        "invalid materializer execution arguments");
  if (flow_msg_payload_validate(msg) != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, SALTS_EINVAL, binding->config_index, "payload",
        "message payload backing is invalid");
  if (flow_msg_has_active_result_claim(msg))
    return flow_plugin_generation_materializer_error(
        error, SALTS_EBUSY, binding->config_index, "execute",
        "message has an active result claim");
  if (turbo_flow_msg_projection(msg, NULL) != NULL)
    return flow_plugin_generation_materializer_error(
        error, SALTS_EBUSY, binding->config_index, "execute",
        "message already has a typed projection");
  descriptor = turbo_flow_msg_content_descriptor(msg);
  rc = flow_plugin_materializer_descriptor_match(binding, descriptor);
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, binding->config_index, "schema",
        "message content descriptor does not match compiled materializer schema");
  if (msg->payload.len > binding->materializer.max_encoded_bytes)
    return flow_plugin_generation_materializer_error(
        error, SALTS_ENOSPC, binding->config_index, "payload",
        "message payload exceeds materializer encoded-byte bound");

  rc = flow_projection_owner_reserve(binding->owner);
  if (rc == SALTS_ECANCELED) rc = SALTS_EBUSY;
  if (rc != SALTS_OK)
    return flow_plugin_generation_materializer_error(
        error, rc, binding->config_index, "capacity",
        "materializer projection capacity is exhausted or closed");

  value = malloc(binding->materializer.native_bytes);
  if (!value) {
    flow_projection_owner_release(binding->owner);
    return flow_plugin_generation_materializer_error(
        error, SALTS_ENOMEM, binding->config_index, "execute",
        "native projection allocation failed");
  }
  memset(value, 0, binding->materializer.native_bytes);
  input.data = msg->payload.data;
  input.data_size = msg->payload.len;
  rc = binding->materializer.materialize(binding->materializer.ctx, &input, value,
                                          binding->materializer.native_bytes);
  if (rc != SALTS_OK) {
    free(value);
    flow_projection_owner_release(binding->owner);
    return flow_plugin_generation_materializer_error(
        error, rc, binding->config_index, "execute",
        "materializer callback failed");
  }

  rc = flow_msg_bind_reserved_typed_projection(
      msg, binding->owner, binding->materializer.data, value);
  if (rc != SALTS_OK) {
    free(value);
    flow_projection_owner_release(binding->owner);
    return flow_plugin_generation_materializer_error(
        error, rc, binding->config_index, "execute",
        "materialized projection could not be bound to the message");
  }
  return SALTS_OK;
}

int turbo_flow_plugin_generation_poll(turbo_flow_plugin_generation_t *generation,
                                      uint32_t timeout_ms, turbo_flow_config_error_t *error) {
  size_t owner_count;
  size_t first = SIZE_MAX;
  if (!generation || !error || error->size < sizeof(*error))
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.generation.poll",
                                        "invalid Graph generation poll arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (generation->failed || generation->poll_closed || !generation->flow ||
      turbo_flow_state(generation->flow) != TURBO_FLOW_STATE_STARTED)
    return flow_plugin_generation_error(error, SALTS_EBUSY, "$.generation.poll",
                                        "Graph generation is not accepting progress");
  owner_count = vec_size(&generation->owners);
  if (owner_count == 0u) return SALTS_OK;
  for (size_t offset = 0u; offset < owner_count; ++offset) {
    const size_t index = (generation->poll_cursor + offset) % owner_count;
    const flow_plugin_generation_owner_t *entry =
        (const flow_plugin_generation_owner_t *)vec_at_const(&generation->owners, index);
    if (entry && (entry->owner.flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL) != 0u) {
      first = index;
      break;
    }
  }
  if (first == SIZE_MAX) return SALTS_OK;
  generation->poll_cursor = (first + 1u) % owner_count;
  for (size_t offset = 0u; offset < owner_count; ++offset) {
    const size_t index = (first + offset) % owner_count;
    const flow_plugin_generation_owner_t *entry =
        (const flow_plugin_generation_owner_t *)vec_at_const(&generation->owners, index);
    int rc;
    if (!entry ||
        (entry->owner.flags & TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL) == 0u)
      continue;
    rc = entry->owner.poll(entry->owner.ctx, index == first ? timeout_ms : 0u);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "poll");
  }
  return SALTS_OK;
}

int turbo_flow_plugin_generation_lease_acquire(turbo_flow_plugin_generation_t *generation) {
  if (!generation) return SALTS_EINVAL;
  if (generation->leases == SIZE_MAX) return SALTS_ENOSPC;
  generation->leases++;
  return SALTS_OK;
}

int turbo_flow_plugin_generation_lease_release(turbo_flow_plugin_generation_t *generation) {
  if (!generation || generation->leases == 0u) return SALTS_EINVAL;
  generation->leases--;
  return SALTS_OK;
}

static int flow_plugin_generation_retire(turbo_flow_plugin_generation_t *generation,
                                         uint64_t timeout_ms, turbo_flow_config_error_t *error) {
  int rc;
  if (!generation || !error || error->size < sizeof(*error))
    return flow_plugin_generation_error(error, SALTS_EINVAL, "$.generation",
                                        "invalid Graph generation destroy arguments");
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (generation->unretirable_owner)
    return flow_plugin_generation_provider_error(
        error, SALTS_EINVAL, generation->unretirable_resource ? "channels" : "adapters",
        generation->unretirable_owner,
        "Product owner has no verifiable cleanup contract; resources remain pinned");
  rc = flow_durable_buffers_prepare_retire(generation->flow, timeout_ms);
  if (rc != SALTS_OK)
    return flow_plugin_generation_error(error, rc, "$.generation.buffers",
                                        "durable backlog must settle before retirement");
  rc = flow_plugin_materializers_stop_and_check(&generation->materializers, error);
  if (rc != SALTS_OK) return rc;
  rc = flow_plugin_operations_close(&generation->bindings);
  if (rc != SALTS_OK)
    return flow_plugin_generation_error(error, rc, "$.generation.inflight",
                                        "operation still executing");
  if (generation->leases != 0u)
    return flow_plugin_generation_error(error, SALTS_EBUSY, "$.generation.leases",
                                        "Graph generation still has active leases");
  generation->poll_closed = 1;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (!entry || entry->state >= FLOW_PLUGIN_GENERATION_OWNER_QUIESCED) continue;
    rc = entry->owner.quiesce(entry->owner.ctx, timeout_ms);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "quiesce");
    entry->state = FLOW_PLUGIN_GENERATION_OWNER_QUIESCED;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_QUIESCED;
  /* A failed create never started execution; compile failure has no running Graph to stop. */
  if (!generation->failed && generation->flow &&
      (turbo_flow_state(generation->flow) == TURBO_FLOW_STATE_STARTED ||
       turbo_flow_state(generation->flow) == TURBO_FLOW_STATE_FAILED)) {
    rc = turbo_flow_stop(generation->flow);
    if (rc != SALTS_OK)
      return flow_plugin_generation_error(error, rc, "$.generation.graph.stop",
                                          "Graph stop failed");
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_STOPPED;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (!entry || entry->state >= FLOW_PLUGIN_GENERATION_OWNER_DRAINED) continue;
    rc = entry->owner.drain(entry->owner.ctx, timeout_ms);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "drain");
    entry->state = FLOW_PLUGIN_GENERATION_OWNER_DRAINED;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_DRAINED;
  rc = flow_plugin_operations_release(&generation->bindings, error);
  if (rc != SALTS_OK) return rc;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (!entry || entry->state >= FLOW_PLUGIN_GENERATION_OWNER_SHUTDOWN) continue;
    rc = entry->owner.shutdown(entry->owner.ctx);
    if (rc != SALTS_OK) return flow_plugin_generation_owner_error(error, rc, entry, "shutdown");
    entry->state = FLOW_PLUGIN_GENERATION_OWNER_SHUTDOWN;
  }
  generation->state = TURBO_FLOW_PLUGIN_GENERATION_SHUTDOWN;
  turbo_flow_destroy(generation->flow);
  generation->flow = NULL;
  for (size_t i = vec_size(&generation->owners); i > 0u; --i) {
    flow_plugin_generation_owner_t *entry =
        (flow_plugin_generation_owner_t *)vec_at(&generation->owners, i - 1u);
    if (entry) entry->owner.destroy(entry->owner.ctx);
  }
  flow_plugin_result_domain_detach(generation->domain);
  rc = flow_plugin_materializers_discard(&generation->materializers);
  if (rc != SALTS_OK)
    return flow_plugin_generation_error(error, rc, "$.generation.materializers",
                                        "materializer projection owner cleanup failed");
  turbo_flow_plugin_catalog_snapshot_destroy(generation->snapshot);
  flow_plugin_operations_free(&generation->bindings);
  vec_destroy(&generation->owners);
  memset(generation, 0, sizeof(*generation));
  free(generation);
  return SALTS_OK;
}

int turbo_flow_plugin_generation_destroy(turbo_flow_plugin_generation_t *generation,
                                         uint64_t timeout_ms, turbo_flow_config_error_t *error) {
  int rc;
  if (!generation || !error || error->size != sizeof(*error)) return SALTS_EINVAL;
  rc = flow_plugin_generation_retire(generation, timeout_ms, error);
  if (rc != SALTS_OK) generation->cleanup_error = *error;
  return rc;
}
int turbo_flow_plugin_generation_cleanup_error(const turbo_flow_plugin_generation_t *generation,
                                               turbo_flow_config_error_t *out) {
  if (!generation || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  *out = generation->cleanup_error;
  return SALTS_OK;
}
int turbo_flow_plugin_generation_operation_error(const turbo_flow_plugin_generation_t *generation,
                                                 size_t index,
                                                 turbo_flow_plugin_operation_error_v3_t *out) {
  flow_plugin_operation_binding_t *binding;
  if (!generation || !out || out->size != sizeof(*out) ||
      out->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      out->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      index >= vec_size(&generation->bindings))
    return SALTS_EINVAL;
  binding = (flow_plugin_operation_binding_t *)vec_at_const(&generation->bindings, index);
  salts_mutex_lock(&binding->error_mutex);
  *out = binding->error;
  salts_mutex_unlock(&binding->error_mutex);
  return SALTS_OK;
}
