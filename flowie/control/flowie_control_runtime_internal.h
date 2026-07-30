#ifndef FLOWIE_CONTROL_RUNTIME_INTERNAL_H
#define FLOWIE_CONTROL_RUNTIME_INTERNAL_H

#include "flowie_control_config_internal.h"
#include "flowie_control_management_service_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_runtime_s flowie_control_runtime_t;

/** Validate TLS identity, secret references, and store selection without opening a listener/DB. */
int flowie_control_runtime_validate(const flowie_control_config_t *config);

/** Create the complete controller composition root and bind all enabled routes. */
int flowie_control_runtime_create(const flowie_control_config_t *config,
                                  flowie_control_runtime_t **out);

/**
 * Start and stop the owned HTTPS listener without installing process signal handlers.
 *
 * These calls are caller-serialized. start() binds the configured endpoint before it returns;
 * stop() asks the owner context to cancel accepted connections, closes the listener there,
 * stops the context, and joins the owner thread.
 */
int flowie_control_runtime_start(flowie_control_runtime_t *runtime);
int flowie_control_runtime_stop(flowie_control_runtime_t *runtime);

/** Run the configured HTTPS/mTLS listener until Iris receives a shutdown signal. */
int flowie_control_runtime_run(flowie_control_runtime_t *runtime);

/**
 * Stop request handling and destroy the selected repository.
 *
 * PostgreSQL returns a close error without freeing the runtime so the caller can retry after
 * outstanding leases are returned.
 */
int flowie_control_runtime_destroy(flowie_control_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
