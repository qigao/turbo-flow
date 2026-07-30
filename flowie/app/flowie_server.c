#include "flowie_server_application_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWIE_SERVER_WAIT_INTERVAL_MS = 100u };

static volatile sig_atomic_t flowie_server_stop_requested = 0;

static void flowie_server_signal(int signal_number) {
  (void)signal_number;
  flowie_server_stop_requested = 1;
}

static void flowie_server_usage(const char *program) {
  (void)fprintf(
      stderr,
      "Usage: %s [--check] [--require-security] [--profile NAME] "
      "[--control-config PATH] [--storage-backend-plugin PATH] "
      "<config.yml> <graph.flow>\n",
      program ? program : "flowie_server");
}

static int flowie_server_report(const flowie_server_application_error_t *error) {
  const char *operation = error && error->operation ? error->operation : "server operation";
  int status = error ? error->status : TURBO_EIO;
  if (error && error->detail == FLOWIE_SERVER_APPLICATION_ERROR_WORKER &&
      error->worker.detail == FLOWIE_WORKER_ERROR_CONFIG &&
      error->worker.config.status != TURBO_OK) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d path=%s message=%s\n", operation,
                  status, error->worker.config.path[0] ? error->worker.config.path : "-",
                  error->worker.config.message[0] ? error->worker.config.message : "-");
  } else if (error && error->detail == FLOWIE_SERVER_APPLICATION_ERROR_WORKER &&
             error->worker.detail == FLOWIE_WORKER_ERROR_FLOW &&
             error->worker.flow.code != TURBO_OK) {
    (void)fprintf(stderr,
                  "flowie_server: %s failed: status=%d line=%u column=%u message=%s\n",
                  operation, status, error->worker.flow.line, error->worker.flow.column,
                  error->worker.flow.message[0] ? error->worker.flow.message : "-");
  } else if (error && error->detail == FLOWIE_SERVER_APPLICATION_ERROR_STORAGE_PLUGIN) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d plugin=%s message=%s\n",
                  operation, status, error->subject[0] ? error->subject : "-",
                  error->message[0] ? error->message : "-");
  } else if (error && error->detail == FLOWIE_SERVER_APPLICATION_ERROR_CONTROL_CONFIG) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d path=%s message=%s\n", operation,
                  status, error->subject[0] ? error->subject : "$",
                  error->message[0] ? error->message : "-");
  } else {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d reason=%s\n", operation, status,
                  turbo_strerror(status));
  }
  return EXIT_FAILURE;
}

int main(int argc, char **argv) {
  flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
  flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
  const char *storage_backend_plugin_paths[FLOWIE_SERVER_STORAGE_BACKEND_PLUGIN_MAX];
  flowie_server_application_t *application = NULL;
  int check_only = 0;
  int result = EXIT_FAILURE;
  int rc;

  config.storage_backend_plugin_paths = storage_backend_plugin_paths;
  for (int index = 1; index < argc; ++index) {
    if (strcmp(argv[index], "--check") == 0) {
      check_only = 1;
    } else if (strcmp(argv[index], "--require-security") == 0) {
      config.require_security = 1;
    } else if (strcmp(argv[index], "--profile") == 0) {
      if (++index >= argc || !argv[index][0]) {
        flowie_server_usage(argv[0]);
        goto done;
      }
      config.profile = argv[index];
    } else if (strcmp(argv[index], "--control-config") == 0) {
      if (++index >= argc || !argv[index][0] || config.control_config_path) {
        flowie_server_usage(argv[0]);
        goto done;
      }
      config.control_config_path = argv[index];
    } else if (strcmp(argv[index], "--storage-backend-plugin") == 0 ||
               strcmp(argv[index], "--record-store-plugin") == 0) {
      if (++index >= argc || !argv[index][0] ||
          config.storage_backend_plugin_path_count >=
              FLOWIE_SERVER_STORAGE_BACKEND_PLUGIN_MAX) {
        flowie_server_usage(argv[0]);
        goto done;
      }
      storage_backend_plugin_paths[config.storage_backend_plugin_path_count++] = argv[index];
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

  rc = flowie_server_application_create(&config, &application, &error);
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
    error.detail = FLOWIE_SERVER_APPLICATION_ERROR_NONE;
    result = flowie_server_report(&error);
    goto done;
  }
  rc = flowie_server_application_start(application, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  (void)fprintf(stdout, "flowie_server: running; press Ctrl+C to stop\n");
  while (!flowie_server_stop_requested)
    turbo_sleep_ms(FLOWIE_SERVER_WAIT_INTERVAL_MS);
  rc = flowie_server_application_stop(application, &error);
  if (rc != TURBO_OK) {
    result = flowie_server_report(&error);
    goto done;
  }
  result = EXIT_SUCCESS;

done:
  rc = flowie_server_application_destroy(application, &error);
  if (rc != TURBO_OK && result == EXIT_SUCCESS) result = flowie_server_report(&error);
  return result;
}
