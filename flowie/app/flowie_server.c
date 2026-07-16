#include "flowie_worker_runtime_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static volatile sig_atomic_t flowie_server_stop_requested = 0;

static void flowie_server_signal(int signal_number) {
  (void)signal_number;
  flowie_server_stop_requested = 1;
}

static void flowie_server_usage(const char *program) {
  (void)fprintf(stderr, "Usage: %s [--check] [--profile NAME] <config.yml> <graph.flow>\n",
                program ? program : "flowie_server");
}

static int flowie_server_report(const flowie_worker_error_t *error) {
  const char *operation = error && error->operation ? error->operation : "worker operation";
  int status = error ? error->status : TURBO_EIO;
  if (error && error->detail == FLOWIE_WORKER_ERROR_CONFIG && error->config.status != TURBO_OK) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d path=%s message=%s\n", operation,
                  status, error->config.path[0] ? error->config.path : "-",
                  error->config.message[0] ? error->config.message : "-");
  } else if (error && error->detail == FLOWIE_WORKER_ERROR_FLOW && error->flow.code != TURBO_OK) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d line=%u column=%u message=%s\n",
                  operation, status, error->flow.line, error->flow.column,
                  error->flow.message[0] ? error->flow.message : "-");
  } else {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d\n", operation, status);
  }
  return EXIT_FAILURE;
}

int main(int argc, char **argv) {
  flowie_worker_runtime_config_t config = FLOWIE_WORKER_RUNTIME_CONFIG_INIT;
  flowie_worker_error_t error = FLOWIE_WORKER_ERROR_INIT;
  flowie_worker_runtime_t *runtime = NULL;
  int check_only = 0;
  int result = EXIT_FAILURE;
  int rc;

  for (int index = 1; index < argc; ++index) {
    if (strcmp(argv[index], "--check") == 0) {
      check_only = 1;
    } else if (strcmp(argv[index], "--profile") == 0) {
      if (++index >= argc || !argv[index][0]) {
        flowie_server_usage(argv[0]);
        goto done;
      }
      config.profile = argv[index];
    } else if (strcmp(argv[index], "--help") == 0 || strcmp(argv[index], "-h") == 0) {
      flowie_server_usage(argv[0]);
      result = EXIT_SUCCESS;
      goto done;
    } else if (!config.config_path) {
      config.config_path = argv[index];
    } else if (!config.graph_path) {
      config.graph_path = argv[index];
    } else {
      flowie_server_usage(argv[0]);
      goto done;
    }
  }
  if (!config.config_path || !config.graph_path) {
    flowie_server_usage(argv[0]);
    goto done;
  }

  rc = flowie_worker_runtime_create(&config, &runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  if (check_only) {
    (void)fprintf(stdout, "flowie_server: configuration and graph are valid\n");
    result = EXIT_SUCCESS;
    goto done;
  }
  if (signal(SIGINT, flowie_server_signal) == SIG_ERR ||
      signal(SIGTERM, flowie_server_signal) == SIG_ERR) {
    error.operation = "install signal handlers";
    error.status = TURBO_EIO;
    error.detail = FLOWIE_WORKER_ERROR_NONE;
    result = flowie_server_report(&error);
    goto done;
  }
  rc = flowie_worker_runtime_start(runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  (void)fprintf(stdout, "flowie_server: running; press Ctrl+C to stop\n");
  while (!flowie_server_stop_requested)
    turbo_sleep_ms(100u);
  rc = flowie_worker_runtime_stop(runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  result = EXIT_SUCCESS;

done:
  rc = flowie_worker_runtime_destroy(runtime, &error);
  if (rc != TURBO_OK && result == EXIT_SUCCESS) result = flowie_server_report(&error);
  return result;
}
