#ifndef FLOWIE_CONTROL_BOOTSTRAP_INTERNAL_H
#define FLOWIE_CONTROL_BOOTSTRAP_INTERNAL_H

#include "flowie_control_config_internal.h"
#include "flowie_control_repository_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Apply the one supported initial-administrator command sequence.
 *
 * The repository and secret are borrowed for the call. Every step has a stable request id and
 * exact expected revision, so an interrupted empty-store bootstrap can resume while unrelated
 * pre-existing state or configuration drift fails closed.
 */
int flowie_control_bootstrap_apply(const flowie_control_repository_t *repository,
                                   const flowie_control_config_bootstrap_t *config,
                                   const void *password, size_t password_size,
                                   uint64_t occurred_at);

#ifdef __cplusplus
}
#endif

#endif
