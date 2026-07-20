#include "flowie_control_startup_options_internal.h"

#include "turbo_error.h"
#include "turbo_parser.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int flowie_control_startup_copy(char *destination, size_t capacity, const char *source,
                                       int required) {
  size_t length;
  if (!destination || capacity == 0u) return TURBO_EINVAL;
  destination[0] = '\0';
  if (!source || !source[0]) return required ? TURBO_EINVAL : TURBO_OK;
  length = strnlen(source, capacity);
  if (length >= capacity) return TURBO_ENAMETOOLONG;
  memcpy(destination, source, length + 1u);
  return TURBO_OK;
}

static int flowie_control_startup_env_file(int argc, char **argv, const char **env_file_out) {
  const char *selected = NULL;
  int found = 0;
  if (env_file_out) *env_file_out = NULL;
  if (argc <= 0 || !argv || !env_file_out) return TURBO_EINVAL;
  for (int index = 1; index < argc; ++index) {
    const char *argument = argv[index];
    if (!argument) return TURBO_EINVAL;
    if (strcmp(argument, "--env-file") == 0 || strcmp(argument, "-E") == 0) {
      if (found || ++index >= argc || !argv[index] || !argv[index][0]) return TURBO_EINVAL;
      selected = argv[index];
      found = 1;
    } else if (strncmp(argument, "--env-file=", sizeof("--env-file=") - 1u) == 0) {
      if (found || argument[sizeof("--env-file=") - 1u] == '\0') return TURBO_EINVAL;
      selected = argument + sizeof("--env-file=") - 1u;
      found = 1;
    }
  }
  if (!selected) selected = getenv(FLOWIE_CONTROL_ENV_ENV_FILE);
  if (selected && strnlen(selected, TURBO_FS_MAX_PATH) >= TURBO_FS_MAX_PATH)
    return TURBO_ENAMETOOLONG;
  *env_file_out = selected;
  return TURBO_OK;
}

int flowie_control_startup_options_parse(int argc, char **argv,
                                         flowie_control_startup_options_t *out) {
  flowie_control_startup_options_t resolved = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
  turbo_cmd_parser_t *parser = NULL;
  const char *selected_env_file = NULL;
  char *config_path = NULL;
  char *env_file = NULL;
  bool check_only = false;
  int rc;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  *out = resolved;
  rc = flowie_control_startup_env_file(argc, argv, &selected_env_file);
  if (rc != TURBO_OK) return rc;
  if (selected_env_file && turbo_dotenv_load(selected_env_file, false) != 0) return TURBO_EIO;

  parser = turbo_cmd_create("flowie-control", "0.1.0");
  if (!parser) return TURBO_ENOMEM;
  turbo_cmd_add_string(parser, &config_path, "config", "c", "Controller configuration file");
  turbo_cmd_set_env(parser, turbo_cmd_last_index(parser), FLOWIE_CONTROL_ENV_CONFIG);
  turbo_cmd_add_string(parser, &env_file, "env-file", "E", "Explicit DotEnv file");
  turbo_cmd_set_env(parser, turbo_cmd_last_index(parser), FLOWIE_CONTROL_ENV_ENV_FILE);
  turbo_cmd_add_flag(parser, &check_only, "check", NULL, "Validate configuration and exit");
  turbo_cmd_set_env(parser, turbo_cmd_last_index(parser), FLOWIE_CONTROL_ENV_CHECK);
  turbo_cmd_parse(parser, argc, argv, false);

  rc = flowie_control_startup_copy(resolved.config_path, sizeof(resolved.config_path), config_path,
                                   1);
  if (rc == TURBO_OK)
    rc = flowie_control_startup_copy(resolved.env_file, sizeof(resolved.env_file), env_file, 0);
  if (rc == TURBO_OK) {
    resolved.check_only = check_only ? 1 : 0;
    *out = resolved;
  }
  turbo_cmd_destroy(parser);
  return rc;
}
