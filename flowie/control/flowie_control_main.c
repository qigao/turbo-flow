#include "flowie_control_config_internal.h"
#include "flowie_control_runtime_internal.h"
#include "flowie_control_startup_options_internal.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>

static int flowie_control_report_config(const flowie_control_config_error_t *error) {
  (void)fprintf(stderr, "flowie-control: configuration failed: status=%d path=%s message=%s\n",
                error ? error->status : TURBO_EINVAL, error && error->path[0] ? error->path : "$",
                error && error->message[0] ? error->message : "invalid configuration");
  return EXIT_FAILURE;
}

static int flowie_control_report_runtime(const char *operation, int status) {
  (void)fprintf(stderr, "flowie-control: %s failed: status=%d\n", operation ? operation : "runtime",
                status);
  return EXIT_FAILURE;
}

int main(int argc, char **argv) {
  flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
  flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
  flowie_control_config_error_t error = FLOWIE_CONTROL_CONFIG_ERROR_INIT;
  flowie_control_runtime_t *runtime = NULL;
  int result = EXIT_FAILURE;
  int rc = flowie_control_startup_options_parse(argc, argv, &options);
  if (rc != TURBO_OK) return flowie_control_report_runtime("startup option parsing", rc);
  rc = flowie_control_config_load(options.config_path, &config, &error);
  if (rc != TURBO_OK) return flowie_control_report_config(&error);
  if (options.check_only) {
    rc = flowie_control_runtime_validate(&config);
    return rc == TURBO_OK ? EXIT_SUCCESS
                          : flowie_control_report_runtime("configuration validation", rc);
  }
  rc = flowie_control_runtime_create(&config, &runtime);
  if (rc != TURBO_OK) return flowie_control_report_runtime("runtime creation", rc);
  rc = flowie_control_runtime_run(runtime);
  if (rc == TURBO_OK) result = EXIT_SUCCESS;
  else result = flowie_control_report_runtime("HTTPS listener", rc);
  rc = flowie_control_runtime_destroy(runtime);
  if (rc != TURBO_OK) result = flowie_control_report_runtime("runtime destruction", rc);
  return result;
}
