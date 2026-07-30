#include "flowie_supervisor_runtime_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static int read_all_stdout(flowie_supervisor_runtime_t *runtime, char *buffer, size_t capacity) {
  size_t total = 0;
  int rc = TURBO_OK;
  while (total + 1u < capacity) {
    size_t count = 0;
    rc = flowie_supervisor_runtime_read_stdout(runtime, buffer + total, capacity - total - 1u,
                                               &count);
    total += count;
    if (rc == TURBO_EOF) break;
    if (rc != TURBO_OK || count == 0) break;
  }
  buffer[total] = '\0';
  return rc;
}

static int read_all_stderr(flowie_supervisor_runtime_t *runtime, char *buffer, size_t capacity) {
  size_t total = 0;
  int rc = TURBO_OK;
  while (total + 1u < capacity) {
    size_t count = 0;
    rc = flowie_supervisor_runtime_read_stderr(runtime, buffer + total, capacity - total - 1u,
                                               &count);
    total += count;
    if (rc == TURBO_EOF) break;
    if (rc != TURBO_OK || count == 0) break;
  }
  buffer[total] = '\0';
  return rc;
}

spec("flowie supervisor runtime") {
  it("rejects an incomplete worker generation") {
    flowie_supervisor_runtime_config_t config = FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT;
    flowie_supervisor_error_t error = FLOWIE_SUPERVISOR_ERROR_INIT;
    flowie_supervisor_runtime_t *runtime = (flowie_supervisor_runtime_t *)1;

    config.worker_program = FLOWIE_TEST_WORKER_PROGRAM;
    check_int_eq(flowie_supervisor_runtime_create(&config, &runtime, &error), TURBO_EINVAL);
    check_null(runtime);
    check_str_eq(error.operation, "validate supervisor configuration");
  }

  it("runs one checked configuration and preserves the child result") {
    flowie_supervisor_runtime_config_t config = FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT;
    flowie_supervisor_error_t error = FLOWIE_SUPERVISOR_ERROR_INIT;
    flowie_supervisor_runtime_t *runtime = NULL;
    turbo_process_result_t child;
    char output[256];
    char profile[sizeof("missing")] = "flowie";

    config.worker_program = FLOWIE_TEST_WORKER_PROGRAM;
    config.profile = profile;
    config.config_path = FLOWIE_TEST_CONFIG_PATH;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    config.check_only = 1;
    config.capture_output = 1;
    check_int_eq(flowie_supervisor_runtime_create(&config, &runtime, &error), TURBO_OK);
    check_not_null(runtime);
    (void)memcpy(profile, "missing", sizeof("missing"));
    check_int_eq(flowie_supervisor_runtime_start(runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_wait_for(runtime, 10000u, &child, &error), TURBO_OK);
    check_int_eq(child.state, TURBO_PROCESS_EXITED);
    check_int_eq(child.exit_code, 0);
    check_int_eq(flowie_supervisor_runtime_start(runtime, &error), TURBO_EALREADY);
    check_int_eq(read_all_stdout(runtime, output, sizeof(output)), TURBO_EOF);
    check_str_contains(output, "configuration and graph are valid");
    flowie_supervisor_runtime_destroy(runtime);
  }

  it("surfaces a worker configuration failure without hiding stderr") {
    flowie_supervisor_runtime_config_t config = FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT;
    flowie_supervisor_error_t error = FLOWIE_SUPERVISOR_ERROR_INIT;
    flowie_supervisor_runtime_t *runtime = NULL;
    turbo_process_result_t child;
    char output[512];

    config.worker_program = FLOWIE_TEST_WORKER_PROGRAM;
    config.profile = "missing";
    config.config_path = FLOWIE_TEST_CONFIG_PATH;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    config.check_only = 1;
    config.capture_output = 1;
    check_int_eq(flowie_supervisor_runtime_create(&config, &runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_start(runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_wait_for(runtime, 10000u, &child, &error), TURBO_OK);
    check_int_eq(child.state, TURBO_PROCESS_EXITED);
    check_int_ne(child.exit_code, 0);
    check_int_eq(read_all_stderr(runtime, output, sizeof(output)), TURBO_EOF);
    check_str_contains(output, "resolve profile failed");
    flowie_supervisor_runtime_destroy(runtime);
  }

  it("forwards the embedded Control configuration to the worker") {
    flowie_supervisor_runtime_config_t config = FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT;
    flowie_supervisor_error_t error = FLOWIE_SUPERVISOR_ERROR_INIT;
    flowie_supervisor_runtime_t *runtime = NULL;
    turbo_process_result_t child;
    char output[512];

    config.worker_program = FLOWIE_TEST_WORKER_PROGRAM;
    config.config_path = FLOWIE_TEST_CONFIG_PATH;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    config.control_config_path = "missing-flowie-control.yml";
    config.check_only = 1;
    config.capture_output = 1;
    check_int_eq(flowie_supervisor_runtime_create(&config, &runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_start(runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_wait_for(runtime, 10000u, &child, &error), TURBO_OK);
    check_int_eq(child.state, TURBO_PROCESS_EXITED);
    check_int_ne(child.exit_code, 0);
    check_int_eq(read_all_stderr(runtime, output, sizeof(output)), TURBO_EOF);
    check_str_contains(output, "load control configuration failed");
    flowie_supervisor_runtime_destroy(runtime);
  }

  it("terminates and reaps a long-running worker on stop") {
    flowie_supervisor_runtime_config_t config = FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT;
    flowie_supervisor_error_t error = FLOWIE_SUPERVISOR_ERROR_INIT;
    flowie_supervisor_runtime_t *runtime = NULL;
    turbo_process_result_t child;

    config.worker_program = FLOWIE_TEST_LONG_RUNNING_WORKER_PROGRAM;
    config.config_path = "unused.yml";
    config.graph_path = "unused.flow";
    check_int_eq(flowie_supervisor_runtime_create(&config, &runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_start(runtime, &error), TURBO_OK);
    check_int_eq(flowie_supervisor_runtime_wait_for(runtime, 50u, &child, &error), TURBO_ETIMEDOUT);
    check_int_eq(flowie_supervisor_runtime_stop(runtime, &child, &error), TURBO_OK);
    check_int_eq(child.state, TURBO_PROCESS_TERMINATED);
    flowie_supervisor_runtime_destroy(runtime);
  }
}
