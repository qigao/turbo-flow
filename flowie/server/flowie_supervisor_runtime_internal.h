#ifndef FLOWIE_SUPERVISOR_RUNTIME_INTERNAL_H
#define FLOWIE_SUPERVISOR_RUNTIME_INTERNAL_H

#include "turbo_process.h"

#include <stddef.h>
#include <stdint.h>

typedef struct flowie_supervisor_runtime_s flowie_supervisor_runtime_t;

#define FLOWIE_SUPERVISOR_DEFAULT_CAPTURE_LIMIT (1024U * 1024U)

typedef struct flowie_supervisor_runtime_config_s {
  size_t size;
  const char *worker_program;
  const char *profile;
  const char *config_path;
  const char *graph_path;
  const char *control_config_path;
  int check_only;
  int require_security;
  int capture_output;
  size_t max_output_bytes;
} flowie_supervisor_runtime_config_t;

#define FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT                                                      \
  {sizeof(flowie_supervisor_runtime_config_t), NULL, "flowie", NULL, NULL, NULL, 0, 0, 0,          \
   FLOWIE_SUPERVISOR_DEFAULT_CAPTURE_LIMIT}

typedef struct flowie_supervisor_error_s {
  size_t size;
  const char *operation;
  int status;
} flowie_supervisor_error_t;

#define FLOWIE_SUPERVISOR_ERROR_INIT {sizeof(flowie_supervisor_error_t), NULL, 0}

/** Create one single-use owner for one worker configuration generation. */
int flowie_supervisor_runtime_create(const flowie_supervisor_runtime_config_t *config,
                                     flowie_supervisor_runtime_t **out,
                                     flowie_supervisor_error_t *error);

/** Spawn the configured Worker directly, without a shell or execution deadline. */
int flowie_supervisor_runtime_start(flowie_supervisor_runtime_t *runtime,
                                    flowie_supervisor_error_t *error);

/** Observe completion without changing the child lifecycle. */
int flowie_supervisor_runtime_wait_for(flowie_supervisor_runtime_t *runtime, uint64_t timeout_ms,
                                       turbo_process_result_t *result,
                                       flowie_supervisor_error_t *error);

/** Terminate and reap the owned Worker. Safe when it has already exited. */
int flowie_supervisor_runtime_stop(flowie_supervisor_runtime_t *runtime,
                                   turbo_process_result_t *result,
                                   flowie_supervisor_error_t *error);

/** Consume captured output; capture must have been enabled in the config. */
int flowie_supervisor_runtime_read_stdout(flowie_supervisor_runtime_t *runtime, void *buffer,
                                          size_t capacity, size_t *out_read);
int flowie_supervisor_runtime_read_stderr(flowie_supervisor_runtime_t *runtime, void *buffer,
                                          size_t capacity, size_t *out_read);

/** Terminate if necessary, reap the process tree, and release copied configuration. */
void flowie_supervisor_runtime_destroy(flowie_supervisor_runtime_t *runtime);

#endif /* FLOWIE_SUPERVISOR_RUNTIME_INTERNAL_H */
