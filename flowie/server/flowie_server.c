#include "flowie_server_application_internal.h"

#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_thread.h"

#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

enum { FLOWIE_SERVER_WAIT_INTERVAL_MS = 100u };

static volatile sig_atomic_t flowie_server_stop_requested = 0;

static void flowie_server_signal(int signal_number) {
  (void)signal_number;
  flowie_server_stop_requested = 1;
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
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d line=%u column=%u message=%s\n",
                  operation, status, error->worker.flow.line, error->worker.flow.column,
                  error->worker.flow.message[0] ? error->worker.flow.message : "-");
  } else if (error && error->detail == FLOWIE_SERVER_APPLICATION_ERROR_STORAGE_PLUGIN) {
    (void)fprintf(stderr, "flowie_server: %s failed: status=%d plugin=%s message=%s\n", operation,
                  status, error->subject[0] ? error->subject : "-",
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
  /* char * (not const char *) to match turbo_cmd_add_string_list signature; borrowed from argv */
  char *plugin_paths[FLOWIE_SERVER_STORAGE_BACKEND_PLUGIN_MAX];
  flowie_server_application_t *application = NULL;
  turbo_cmd_parser_t *parser = NULL;
  char *config_path = NULL;
  char *graph_path = NULL;
  char *profile = NULL;
  char *control_config_path = NULL;
  char *protocol_store_path = NULL;
  uint32_t plugin_path_count = 0;
  bool check_only = false;
  bool require_security = false;
  int result = EXIT_FAILURE;
  int rc;

  parser = turbo_cmd_create("flowie_server", "");
  if (!parser) {
    (void)fprintf(stderr, "flowie_server: failed to create argument parser\n");
    return EXIT_FAILURE;
  }
  turbo_cmd_add_flag(parser, &check_only, "check", NULL,
                     "Validate configuration and graph, then exit");
  turbo_cmd_add_flag(parser, &require_security, "require-security", NULL,
                     "Require a security policy to be configured");
  turbo_cmd_add_string(parser, &profile, "profile", NULL,
                       "Named configuration profile (default: flowie)");
  turbo_cmd_add_string(parser, &control_config_path, "control-config", NULL,
                       "Path to the control runtime configuration file");
  turbo_cmd_add_string(parser, &protocol_store_path, "protocol-store-path", NULL,
                       "Standalone MQTT protocol SQLite path (must be :memory:)");
  turbo_cmd_add_string_list(parser, plugin_paths, &plugin_path_count,
                            FLOWIE_SERVER_STORAGE_BACKEND_PLUGIN_MAX, "storage-backend-plugin",
                            NULL, "Storage backend plugin path (repeatable, max 8)");
  turbo_cmd_add_required_string(parser, &config_path, "config.yml", "Server configuration file");
  turbo_cmd_add_required_string(parser, &graph_path, "graph.flow", "Flow graph definition file");

  /* turbo_cmd_parse exits on --help, unknown flags, or missing required positionals */
  turbo_cmd_parse(parser, argc, argv, false);
  turbo_cmd_destroy(parser);

  /* Restore the 'flowie' default profile when --profile is not supplied */
  config.profile = profile ? profile : "flowie";
  config.config_path = config_path;
  config.graph_path = graph_path;
  config.protocol_store_path =
      protocol_store_path ? protocol_store_path : FLOWIE_SERVER_DEFAULT_PROTOCOL_STORE_PATH;
  config.control_config_path = control_config_path;
  config.storage_backend_plugin_paths = (const char **)plugin_paths;
  config.storage_backend_plugin_path_count = plugin_path_count;
  config.require_security = require_security ? 1 : 0;

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
