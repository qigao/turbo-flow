#include "flowie_control_startup_options_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

static int startup_test_env_set(const char *name, const char *value) {
#ifdef _WIN32
  return _putenv_s(name, value ? value : "");
#else
  return value ? setenv(name, value, 1) : unsetenv(name);
#endif
}

static int startup_test_env_clear(void) {
  if (startup_test_env_set(FLOWIE_CONTROL_ENV_CONFIG, NULL) != 0) return -1;
  if (startup_test_env_set(FLOWIE_CONTROL_ENV_ENV_FILE, NULL) != 0) return -1;
  return startup_test_env_set(FLOWIE_CONTROL_ENV_CHECK, NULL);
}

spec("Flowie control startup options") {
  before_each() { check_int_eq(startup_test_env_clear(), 0); }
  after_each() { check_int_eq(startup_test_env_clear(), 0); }

  it("requires an explicit controller configuration source") {
    flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
    char *argv[] = {"flowie-control"};

    check_int_eq(flowie_control_startup_options_parse(1, argv, &options), TURBO_EINVAL);
    check_str_eq(options.config_path, "");
    check_false(options.check_only);
  }

  it("loads controller options from an explicitly selected DotEnv file") {
    static const char content[] =
        FLOWIE_CONTROL_ENV_CONFIG "=dotenv-control.yml\n" FLOWIE_CONTROL_ENV_CHECK "=true\n";
    flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
    char *path = tt_make_temp_file("flowie-control", ".env");
    char *argv[] = {"flowie-control", "--env-file", path};

    check_not_null(path);
    check_int_eq(tt_write_file(path, content, sizeof(content) - 1u), 0);
    check_int_eq(flowie_control_startup_options_parse(3, argv, &options), TURBO_OK);
    check_str_eq(options.config_path, "dotenv-control.yml");
    check_str_eq(options.env_file, path);
    check_true(options.check_only);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("preserves process environment values when DotEnv contains the same key") {
    static const char content[] = FLOWIE_CONTROL_ENV_CONFIG "=dotenv-control.yml\n";
    flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
    char *path = tt_make_temp_file("flowie-control", ".env");
    char *argv[] = {"flowie-control", "-E", path};

    check_not_null(path);
    check_int_eq(tt_write_file(path, content, sizeof(content) - 1u), 0);
    check_int_eq(startup_test_env_set(FLOWIE_CONTROL_ENV_CONFIG, "process-control.yml"), 0);
    check_int_eq(flowie_control_startup_options_parse(3, argv, &options), TURBO_OK);
    check_str_eq(options.config_path, "process-control.yml");
    check_str_eq(options.env_file, path);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("lets command line values override process environment") {
    flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
    char *argv[] = {"flowie-control", "--config", "cli-control.yml", "--check"};

    check_int_eq(startup_test_env_set(FLOWIE_CONTROL_ENV_CONFIG, "process-control.yml"), 0);
    check_int_eq(startup_test_env_set(FLOWIE_CONTROL_ENV_CHECK, "false"), 0);
    check_int_eq(flowie_control_startup_options_parse(4, argv, &options), TURBO_OK);
    check_str_eq(options.config_path, "cli-control.yml");
    check_true(options.check_only);
  }

  it("fails when an explicitly selected DotEnv file cannot be loaded") {
    flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
    char *path = tt_make_temp_file("missing-flowie-control", ".env");
    char *argv[] = {"flowie-control", "--env-file", path};

    check_not_null(path);
    check_int_eq(tt_remove_file(path), 0);
    check_int_eq(flowie_control_startup_options_parse(3, argv, &options), TURBO_EIO);
    check_str_eq(options.config_path, "");
    check_str_eq(options.env_file, "");
    free(path);
  }

  it("rejects configuration paths that exceed the bounded result") {
    flowie_control_startup_options_t options = FLOWIE_CONTROL_STARTUP_OPTIONS_INIT;
    char overlong[TURBO_FS_MAX_PATH + 1u];
    char *argv[] = {"flowie-control", "--config", overlong};

    memset(overlong, 'a', sizeof(overlong) - 1u);
    overlong[sizeof(overlong) - 1u] = '\0';
    check_int_eq(flowie_control_startup_options_parse(3, argv, &options), TURBO_ENAMETOOLONG);
    check_str_eq(options.config_path, "");
  }
}
