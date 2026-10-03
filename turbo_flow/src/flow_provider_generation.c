#include "flow_provider_generation_internal.h"

#include "turbo_flow_stl_error_internal.h"

#include <cstl/vec.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum flow_provider_generation_owner_state_e {
  FLOW_PROVIDER_GENERATION_PREPARED = 1,
  FLOW_PROVIDER_GENERATION_MATERIALIZED,
  FLOW_PROVIDER_GENERATION_QUIESCED,
  FLOW_PROVIDER_GENERATION_DRAINED,
  FLOW_PROVIDER_GENERATION_SHUTDOWN,
  FLOW_PROVIDER_GENERATION_OWNER_DESTROYED
} flow_provider_generation_owner_state_t;

typedef struct flow_provider_generation_entry_s {
  flow_compiled_provider_instance_t *compiled;
  turbo_flow_runtime_owner *owner;
  char *name;
  flow_provider_generation_owner_state_t state;
} flow_provider_generation_entry_t;

struct flow_provider_generation_s {
  vec_t entries;
  size_t poll_cursor;
};

static int provider_generation_error(
    turbo_flow_config_error_t *error, int status,
    const char *stage_name, const char *phase, const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(
        error->path, sizeof(error->path), "$.stages.%s%s%s",
        stage_name ? stage_name : "",
        phase && phase[0] ? "." : "", phase ? phase : "");
    (void)snprintf(
        error->message, sizeof(error->message), "%s",
        message ? message : "provider generation failure");
  }
  return status;
}

static char *provider_generation_copy_text(const char *text) {
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

static const char *provider_generation_identity(
    const turbo_flow_stage_plan_t *stage) {
  if (!stage) return NULL;
  return stage->is_buffer ? stage->provider_name : stage->adapter_name;
}

static int provider_generation_same_text(
    const char *left, const char *right) {
  return left && right && strcmp(left, right) == 0;
}

static int provider_generation_source_has_terminal_companion(
    const turbo_flow_t *flow, size_t stage_index,
    const turbo_flow_stage_plan_t *stage) {
  const char *identity;
  const char *resource_name;
  if (!flow || !stage || !stage->is_source ||
      !stage->resource_name || !stage->resource_name[0])
    return 0;
  identity = stage->adapter_name;
  resource_name = stage->resource_name;
  if (!identity || !identity[0]) return 0;

  /*
   * turbo_flow_stage_at() reuses one thread-local view. Only retain the
   * Graph-owned string pointers above while scanning companion stages.
   */
  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *candidate;
    if (i == stage_index) continue;
    candidate = turbo_flow_stage_at(flow, i);
    if (!candidate || candidate->is_source || candidate->is_buffer)
      continue;
    if (provider_generation_same_text(
            candidate->adapter_name, identity) &&
        provider_generation_same_text(
            candidate->resource_name, resource_name))
      return 1;
  }
  return 0;
}

static int provider_generation_is_root(
    const turbo_flow_t *flow, size_t stage_index,
    const turbo_flow_stage_plan_t *stage) {
  const char *identity = provider_generation_identity(stage);
  if (!stage || !identity || !identity[0]) return 0;
  if (stage->is_buffer) return 1;
  if (!stage->is_source) return 1;
  return !provider_generation_source_has_terminal_companion(
      flow, stage_index, stage);
}

static void provider_generation_entry_discard_prepared(
    flow_provider_generation_entry_t *entry) {
  if (!entry) return;
  if (entry->compiled &&
      entry->state == FLOW_PROVIDER_GENERATION_PREPARED)
    (void)flow_compiled_provider_instance_release(&entry->compiled);
  free(entry->name);
  memset(entry, 0, sizeof(*entry));
}

static int provider_generation_release_prepared(
    flow_provider_generation_t *generation) {
  int first = SALTS_OK;
  if (!generation) return SALTS_OK;
  for (size_t i = vec_size(&generation->entries); i > 0u; --i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i - 1u);
    int rc;
    if (!entry || !entry->compiled ||
        entry->state != FLOW_PROVIDER_GENERATION_PREPARED)
      continue;
    rc = flow_compiled_provider_instance_release(&entry->compiled);
    if (rc != SALTS_OK && first == SALTS_OK) first = rc;
    if (rc == SALTS_OK) {
      free(entry->name);
      entry->name = NULL;
      memset(entry, 0, sizeof(*entry));
    }
  }
  return first;
}

static void provider_generation_free_empty(
    flow_provider_generation_t *generation) {
  if (!generation) return;
  vec_destroy(&generation->entries);
  memset(generation, 0, sizeof(*generation));
  free(generation);
}

int flow_provider_generation_prepare(
    turbo_flow_t *flow,
    const turbo_flow_provider_resolver_v1_t *provider_resolver,
    const turbo_flow_resource_resolver_v1_t *resource_resolver,
    size_t owner_capacity,
    flow_provider_generation_t **out,
    turbo_flow_config_error_t *error) {
  flow_provider_generation_t *generation;
  size_t roots = 0u;
  int rc;

  if (out) *out = NULL;
  if (!flow || !provider_resolver ||
      provider_resolver->size != sizeof(*provider_resolver) ||
      !provider_resolver->resolve || !out)
    return provider_generation_error(
        error, SALTS_EINVAL, NULL, NULL,
        "invalid canonical provider generation arguments");

  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (provider_generation_is_root(flow, i, stage)) {
      if (roots == SIZE_MAX)
        return provider_generation_error(
            error, SALTS_ERANGE, NULL, "capacity",
            "provider root count overflow");
      ++roots;
    }
  }
  if (roots > owner_capacity)
    return provider_generation_error(
        error, SALTS_ENOSPC, NULL, "capacity",
        "provider owner capacity is exhausted");

  generation =
      (flow_provider_generation_t *)calloc(1u, sizeof(*generation));
  if (!generation)
    return provider_generation_error(
        error, SALTS_ENOMEM, NULL, "allocate",
        "provider generation allocation failed");

  rc = turbo_flow_stl_error(vec_init_bytes(
      &generation->entries, sizeof(flow_provider_generation_entry_t),
      _Alignof(flow_provider_generation_entry_t), owner_capacity));
  if (rc == SALTS_OK && roots != 0u)
    rc = turbo_flow_stl_error(vec_reserve(&generation->entries, roots));
  if (rc != SALTS_OK) {
    provider_generation_free_empty(generation);
    return provider_generation_error(
        error, rc, NULL, "allocate",
        "provider generation entry allocation failed");
  }

  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    flow_provider_generation_entry_t entry;
    if (!provider_generation_is_root(flow, i, stage)) continue;

    /* Reacquire after root classification because companion scanning may have
       overwritten the thread-local stage view. */
    stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->name || !stage->name[0]) {
      rc = SALTS_EPROTO;
      provider_generation_error(
          error, rc, NULL, "stage",
          "provider root lost its canonical stage identity");
      goto fail;
    }
    memset(&entry, 0, sizeof(entry));

    entry.name = provider_generation_copy_text(stage->name);
    if (!entry.name) {
      rc = SALTS_ENOMEM;
      provider_generation_error(
          error, rc, stage ? stage->name : NULL, "allocate",
          "provider instance name allocation failed");
      goto fail;
    }

    rc = flow_compiled_provider_instance_prepare(
        flow, i, provider_resolver, resource_resolver,
        &entry.compiled, error);
    if (rc != SALTS_OK) {
      free(entry.name);
      entry.name = NULL;
      goto fail;
    }
    entry.state = FLOW_PROVIDER_GENERATION_PREPARED;

    rc = turbo_flow_stl_error(vec_push(&generation->entries, &entry));
    if (rc != SALTS_OK) {
      provider_generation_entry_discard_prepared(&entry);
      provider_generation_error(
          error, rc, stage->name, "retain",
          "provider generation could not retain compiled instance");
      goto fail;
    }
  }

  *out = generation;
  return SALTS_OK;

fail:
  {
    int cleanup = provider_generation_release_prepared(generation);
    if (cleanup == SALTS_OK) {
      provider_generation_free_empty(generation);
      return rc;
    }
    *out = generation;
    return provider_generation_error(
        error, cleanup, NULL, "cleanup",
        "provider generation cleanup failed after preflight rejection");
  }
}

int flow_provider_generation_materialize(
    flow_provider_generation_t *generation,
    turbo_flow_t *flow,
    turbo_flow_config_error_t *error) {
  if (!generation || !flow)
    return provider_generation_error(
        error, SALTS_EINVAL, NULL, "materialize",
        "invalid provider materialization arguments");

  for (size_t i = 0u; i < vec_size(&generation->entries); ++i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i);
    int rc;
    if (!entry || !entry->compiled) return SALTS_EPROTO;
    if (entry->state >= FLOW_PROVIDER_GENERATION_MATERIALIZED)
      continue;

    rc = flow_compiled_provider_instance_materialize(
        entry->compiled, flow, error);
    if (rc != SALTS_OK) {
      if (!error || error->size != sizeof(*error) ||
          error->status == SALTS_OK)
        provider_generation_error(
            error, rc, entry->name, "materialize",
            "provider materialization failed");
      return rc;
    }
    rc = flow_compiled_provider_instance_owner(
        entry->compiled, &entry->owner);
    if (rc != SALTS_OK ||
        !turbo_flow_runtime_owner_contract_valid(entry->owner))
      return provider_generation_error(
          error, rc != SALTS_OK ? rc : SALTS_EPROTO,
          entry->name, "owner",
          "provider returned an invalid runtime owner");
    entry->state = FLOW_PROVIDER_GENERATION_MATERIALIZED;
  }
  return SALTS_OK;
}

size_t flow_provider_generation_count(
    const flow_provider_generation_t *generation) {
  return generation ? vec_size(&generation->entries) : 0u;
}

int flow_provider_generation_poll(
    flow_provider_generation_t *generation,
    uint32_t timeout_ms,
    turbo_flow_config_error_t *error) {
  size_t count;
  size_t first = SIZE_MAX;
  if (!generation)
    return provider_generation_error(
        error, SALTS_EINVAL, NULL, "poll",
        "invalid provider generation");

  count = vec_size(&generation->entries);
  if (count == 0u) return SALTS_OK;

  for (size_t offset = 0u; offset < count; ++offset) {
    size_t index = (generation->poll_cursor + offset) % count;
    const flow_provider_generation_entry_t *entry =
        (const flow_provider_generation_entry_t *)vec_at_const(
            &generation->entries, index);
    if (entry && entry->state == FLOW_PROVIDER_GENERATION_MATERIALIZED &&
        entry->owner &&
        turbo_flow_runtime_owner_has(
            entry->owner, TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL)) {
      first = index;
      break;
    }
  }
  if (first == SIZE_MAX) return SALTS_OK;

  generation->poll_cursor = (first + 1u) % count;
  for (size_t offset = 0u; offset < count; ++offset) {
    size_t index = (first + offset) % count;
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, index);
    int rc;
    if (!entry ||
        entry->state != FLOW_PROVIDER_GENERATION_MATERIALIZED ||
        !entry->owner ||
        !turbo_flow_runtime_owner_has(
            entry->owner, TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL))
      continue;
    rc = turbo_flow_runtime_owner_poll(
        entry->owner, index == first ? timeout_ms : 0u);
    if (rc != SALTS_OK)
      return provider_generation_error(
          error, rc, entry->name, "poll",
          "provider runtime owner poll failed");
  }
  return SALTS_OK;
}

int flow_provider_generation_quiesce(
    flow_provider_generation_t *generation,
    uint64_t timeout_ms,
    turbo_flow_config_error_t *error) {
  if (!generation) return SALTS_EINVAL;
  for (size_t i = vec_size(&generation->entries); i > 0u; --i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i - 1u);
    int rc;
    if (!entry || entry->state == FLOW_PROVIDER_GENERATION_PREPARED)
      continue;
    if (entry->state >= FLOW_PROVIDER_GENERATION_QUIESCED)
      continue;
    rc = turbo_flow_runtime_owner_quiesce(entry->owner, timeout_ms);
    if (rc != SALTS_OK)
      return provider_generation_error(
          error, rc, entry->name, "quiesce",
          "provider runtime owner quiesce failed");
    entry->state = FLOW_PROVIDER_GENERATION_QUIESCED;
  }
  return SALTS_OK;
}

int flow_provider_generation_drain(
    flow_provider_generation_t *generation,
    uint64_t timeout_ms,
    turbo_flow_config_error_t *error) {
  if (!generation) return SALTS_EINVAL;
  for (size_t i = vec_size(&generation->entries); i > 0u; --i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i - 1u);
    int rc;
    if (!entry || entry->state == FLOW_PROVIDER_GENERATION_PREPARED)
      continue;
    if (entry->state < FLOW_PROVIDER_GENERATION_QUIESCED)
      return provider_generation_error(
          error, SALTS_EBUSY, entry->name, "drain",
          "provider owner must quiesce before drain");
    if (entry->state >= FLOW_PROVIDER_GENERATION_DRAINED)
      continue;
    rc = turbo_flow_runtime_owner_drain(entry->owner, timeout_ms);
    if (rc != SALTS_OK)
      return provider_generation_error(
          error, rc, entry->name, "drain",
          "provider runtime owner drain failed");
    entry->state = FLOW_PROVIDER_GENERATION_DRAINED;
  }
  return SALTS_OK;
}

int flow_provider_generation_shutdown(
    flow_provider_generation_t *generation,
    turbo_flow_config_error_t *error) {
  if (!generation) return SALTS_EINVAL;
  for (size_t i = vec_size(&generation->entries); i > 0u; --i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i - 1u);
    int rc;
    if (!entry || entry->state == FLOW_PROVIDER_GENERATION_PREPARED)
      continue;
    if (entry->state < FLOW_PROVIDER_GENERATION_DRAINED)
      return provider_generation_error(
          error, SALTS_EBUSY, entry->name, "shutdown",
          "provider owner must drain before shutdown");
    if (entry->state >= FLOW_PROVIDER_GENERATION_SHUTDOWN)
      continue;
    rc = turbo_flow_runtime_owner_shutdown(entry->owner);
    if (rc != SALTS_OK)
      return provider_generation_error(
          error, rc, entry->name, "shutdown",
          "provider runtime owner shutdown failed");
    entry->state = FLOW_PROVIDER_GENERATION_SHUTDOWN;
  }
  return SALTS_OK;
}

int flow_provider_generation_owner_destroy(
    flow_provider_generation_t *generation,
    turbo_flow_config_error_t *error) {
  if (!generation) return SALTS_EINVAL;
  for (size_t i = vec_size(&generation->entries); i > 0u; --i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i - 1u);
    int rc;
    if (!entry || entry->state == FLOW_PROVIDER_GENERATION_PREPARED ||
        entry->state >= FLOW_PROVIDER_GENERATION_OWNER_DESTROYED)
      continue;
    if (entry->state < FLOW_PROVIDER_GENERATION_SHUTDOWN)
      return provider_generation_error(
          error, SALTS_EBUSY, entry->name, "destroy",
          "provider owner must shut down before destroy");
    rc = flow_compiled_provider_instance_owner_destroy(entry->compiled);
    if (rc != SALTS_OK)
      return provider_generation_error(
          error, rc, entry->name, "destroy",
          "provider runtime owner destroy failed");
    entry->owner = NULL;
    entry->state = FLOW_PROVIDER_GENERATION_OWNER_DESTROYED;
  }
  return SALTS_OK;
}

int flow_provider_generation_release(
    flow_provider_generation_t **generation_io,
    turbo_flow_config_error_t *error) {
  flow_provider_generation_t *generation;
  int first = SALTS_OK;

  if (!generation_io || !*generation_io) return SALTS_EINVAL;
  generation = *generation_io;

  for (size_t i = vec_size(&generation->entries); i > 0u; --i) {
    flow_provider_generation_entry_t *entry =
        (flow_provider_generation_entry_t *)vec_at(
            &generation->entries, i - 1u);
    int rc;
    if (!entry || !entry->compiled) continue;
    if (entry->state != FLOW_PROVIDER_GENERATION_PREPARED &&
        entry->state < FLOW_PROVIDER_GENERATION_OWNER_DESTROYED)
      return provider_generation_error(
          error, SALTS_EBUSY, entry->name, "release",
          "provider owner is still live");

    rc = flow_compiled_provider_instance_release(&entry->compiled);
    if (rc != SALTS_OK) {
      if (first == SALTS_OK) {
        first = rc;
        provider_generation_error(
            error, rc, entry->name, "release",
            "provider/resource lease release failed");
      }
      continue;
    }
    free(entry->name);
    entry->name = NULL;
    memset(entry, 0, sizeof(*entry));
  }

  if (first != SALTS_OK) return first;

  provider_generation_free_empty(generation);
  *generation_io = NULL;
  return SALTS_OK;
}
