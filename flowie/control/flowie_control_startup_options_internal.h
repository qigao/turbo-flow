#ifndef FLOWIE_CONTROL_STARTUP_OPTIONS_INTERNAL_H
#define FLOWIE_CONTROL_STARTUP_OPTIONS_INTERNAL_H

#include "turbo_fs.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_ENV_CONFIG "FLOWIE_CONTROL_CONFIG"
#define FLOWIE_CONTROL_ENV_ENV_FILE "FLOWIE_CONTROL_ENV_FILE"
#define FLOWIE_CONTROL_ENV_CHECK "FLOWIE_CONTROL_CHECK"

typedef struct flowie_control_startup_options_s {
  size_t size;
  char config_path[TURBO_FS_MAX_PATH];
  char env_file[TURBO_FS_MAX_PATH];
  int check_only;
} flowie_control_startup_options_t;

#define FLOWIE_CONTROL_STARTUP_OPTIONS_INIT                                                       \
  {sizeof(flowie_control_startup_options_t), {0}, {0}, 0}

/**
 * Resolve process startup options once, before threads are created.
 *
 * Precedence is CLI > process environment > explicitly selected DotEnv file.
 * DotEnv loading never overwrites an existing process environment variable.
 * Command syntax/help behavior is owned by TurboUtils CMD parser.
 */
int flowie_control_startup_options_parse(int argc, char **argv,
                                         flowie_control_startup_options_t *out);

#ifdef __cplusplus
}
#endif

#endif
