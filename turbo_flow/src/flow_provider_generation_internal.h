#ifndef FLOW_PROVIDER_GENERATION_INTERNAL_H
#define FLOW_PROVIDER_GENERATION_INTERNAL_H

#include "flow_provider_instance_internal.h"

#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flow_provider_generation_s flow_provider_generation_t;

/**
 * Prepare every canonical provider materialization root without side effects.
 *
 * Root selection is stage-scoped:
 * - every buffer provider is a root;
 * - every non-source adapter stage is a root;
 * - a source adapter is a root unless one non-source stage declares the exact
 *   same provider identity and explicit resource reference. In that paired
 *   shape the terminal stage owns the materialization and may bind both exact
 *   stages through the provider adapter contract.
 *
 * Success retains exactly one Salts Component generation scope for every
 * provider root plus any domain-specific resource bindings and typed config.
 * No provider materialize callback has run yet.
 */
int flow_provider_generation_prepare(
    turbo_flow_t *flow,
    salts_component_plugin_runtime *component_runtime,
    const turbo_flow_provider_resolver_v2_t *provider_resolver,
    const turbo_flow_resource_resolver_v1_t *resource_resolver,
    size_t owner_capacity,
    flow_provider_generation_t **out,
    turbo_flow_config_error_t *error);

/** Materialize all prepared roots in deterministic Graph stage order. */
int flow_provider_generation_materialize(
    flow_provider_generation_t *generation,
    turbo_flow_t *flow,
    turbo_flow_config_error_t *error);

/** Number of provider materialization roots retained by this generation. */
size_t flow_provider_generation_count(
    const flow_provider_generation_t *generation);

/** Exact Salts Component generation pinned by every provider binding. */
uint64_t flow_provider_generation_component_generation_id(
    const flow_provider_generation_t *generation);

/** One bounded external-poll round over materialized owners. */
int flow_provider_generation_poll(
    flow_provider_generation_t *generation,
    uint32_t timeout_ms,
    turbo_flow_config_error_t *error);

/** Retryable reverse-order lifecycle transitions. */
int flow_provider_generation_quiesce(
    flow_provider_generation_t *generation,
    uint64_t timeout_ms,
    turbo_flow_config_error_t *error);
int flow_provider_generation_drain(
    flow_provider_generation_t *generation,
    uint64_t timeout_ms,
    turbo_flow_config_error_t *error);
int flow_provider_generation_shutdown(
    flow_provider_generation_t *generation,
    turbo_flow_config_error_t *error);

/**
 * Destroy materialized runtime owners after Graph teardown.
 *
 * Component scope, resource bindings and typed config remain retained until
 * this succeeds.
 */
int flow_provider_generation_owner_destroy(
    flow_provider_generation_t *generation,
    turbo_flow_config_error_t *error);

/**
 * Release typed config/resource bindings, then release the one retained
 * Component scope and free the generation.
 *
 * Materialized owners must already have been destroyed. On release failure the
 * generation remains live for explicit retry.
 */
int flow_provider_generation_release(
    flow_provider_generation_t **generation_io,
    turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* FLOW_PROVIDER_GENERATION_INTERNAL_H */
