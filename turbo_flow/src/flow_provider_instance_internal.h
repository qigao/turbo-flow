#ifndef TURBO_FLOW_PROVIDER_INSTANCE_INTERNAL_H
#define TURBO_FLOW_PROVIDER_INSTANCE_INTERNAL_H

#include "flow_provider_config_internal.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flow_compiled_provider_instance_s
    flow_compiled_provider_instance_t;

/**
 * Bind one exact .flow stage to its canonical provider/config/resource and
 * complete provider preflight before any provider materialization side effect.
 *
 * Success owns provider/resource Salts Plugin leases plus the typed native
 * config until flow_compiled_provider_instance_release().
 *
 * Normal failure leaves *out NULL. If cleanup itself fails, *out remains
 * non-NULL so the caller can retry release; there is no force-release path.
 */
TURBO_FLOW_C_API int flow_compiled_provider_instance_prepare(
    turbo_flow_t *flow,
    size_t stage_index,
    const turbo_flow_provider_resolver_v1_t *provider_resolver,
    const turbo_flow_resource_resolver_v1_t *resource_resolver,
    flow_compiled_provider_instance_t **out,
    turbo_flow_config_error_t *error);

/** Borrow the exact provider-facing instance after successful preflight. */
TURBO_FLOW_C_API int flow_compiled_provider_instance_view(
    const flow_compiled_provider_instance_t *compiled,
    const turbo_flow_provider_instance_v1_t **out);

/**
 * Materialize exactly once through the already-retained provider binding.
 *
 * The runtime owner is stored inside the compiled instance so provider/resource
 * leases and provider-owned config metadata remain alive for every owner
 * callback. A successful materialization must be followed by explicit owner
 * destruction before the compiled instance can be released.
 */
TURBO_FLOW_C_API int flow_compiled_provider_instance_materialize(
    flow_compiled_provider_instance_t *compiled,
    turbo_flow_t *flow,
    turbo_flow_config_error_t *error);

/**
 * Borrow the materialized runtime owner for quiesce/drain/shutdown/poll.
 * Do not call turbo_flow_runtime_owner_destroy() directly; destruction must go
 * through flow_compiled_provider_instance_owner_destroy() so lease release is
 * ordered after the final provider-owned callback.
 */
TURBO_FLOW_C_API int flow_compiled_provider_instance_owner(
    flow_compiled_provider_instance_t *compiled,
    turbo_flow_runtime_owner **out);

/** Destroy the runtime owner while its provider/resource leases are retained. */
TURBO_FLOW_C_API int flow_compiled_provider_instance_owner_destroy(
    flow_compiled_provider_instance_t *compiled);

/**
 * Destroy typed config, release resource/provider leases, and free the object.
 * Returns SALTS_EBUSY while a materialized runtime owner is still live.
 * On lease release failure, *compiled_io remains live for explicit retry.
 */
TURBO_FLOW_C_API int flow_compiled_provider_instance_release(
    flow_compiled_provider_instance_t **compiled_io);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_INSTANCE_INTERNAL_H */
