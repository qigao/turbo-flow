#include "flowie_supervisor_runtime_internal.h"

#include "turbo_error.h"
#include "turbo_fs.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #define FLOWIE_WORKER_FILENAME "flowie_server.exe"
#else
  #define FLOWIE_WORKER_FILENAME "flowie_server"
#endif

enum { FLOWIE_SUPERVISOR_WAIT_INTERVAL_MS = 100U, FLOWIE_SUPERVISOR_IO_CHUNK = 4096U };

static volatile sig_atomic_t flowie_supervisor_stop_requested = 0;

static void flowie_supervisor_signal(int signal_number) {
  (void)signal_number;
  flowie_supervisor_stop_requested = 1;
}

static void flowie_supervisor_usage(const char *program) {
  (void)fprintf(stderr,
                "Usage: %s [--check] [--require-security] [--profile NAME] [--worker PATH] "
                "[--control-config PATH] [--capture-output BYTES] "
                "<config.yml> <graph.flow>\n",
                program ? program : "flowie_supervisor");
}

static int flowie_supervisor_parse_size(const char *text, size_t *out) {
  unsigned long long value;
  char *end = NULL;
  if (!text || !text[0] || !out) return TURBO_EINVAL;
  errno = 0;
  value = strtoull(text, &end, 10);
  if (errno == ERANGE || !end || *end != '\0' || value == 0 || value > SIZE_MAX)
    return TURBO_EINVAL;
  *out = (size_t)value;
  return TURBO_OK;
}

static int flowie_supervisor_default_worker(const char *program, char *buffer, size_t capacity) {
  char directory[TURBO_FS_MAX_PATH];
  if (!program || !program[0] || !buffer || capacity == 0) return TURBO_EINVAL;
  if (!strchr(program, '/') && !strchr(program, '\\')) {
    if (strlen(FLOWIE_WORKER_FILENAME) + 1u > capacity) return TURBO_ENOSPC;
    (void)memcpy(buffer, FLOWIE_WORKER_FILENAME, strlen(FLOWIE_WORKER_FILENAME) + 1u);
    return TURBO_OK;
  }
  if (turbo_fs_path_dirname(program, directory, sizeof(directory)) != TURBO_OK ||
      turbo_fs_path_join(buffer, capacity, directory, FLOWIE_WORKER_FILENAME) != TURBO_OK)
    return TURBO_ENOSPC;
  return TURBO_OK;
}

static int flowie_supervisor_report(const flowie_supervisor_error_t *error) {
  const char *operation = error && error->operation ? error->operation : "supervisor operation";
  int status = error ? error->status : TURBO_EIO;
  (void)fprintf(stderr, "flowie_supervisor: %s failed: status=%d reason=%s\n", operation, status,
                turbo_strerror(status));
  return EXIT_FAILURE;
}

static void flowie_supervisor_report_child(const turbo_process_result_t *result) {
  if (!result) return;
  (void)fprintf(stderr,
                "flowie_supervisor: worker stopped: state=%s pid=%d exit_code=%d signal=%d "
                "error=%d\n",
                turbo_process_state_name(result->state), result->pid, result->exit_code,
                result->term_signal, result->error_code);
}

static int flowie_supervisor_forward_stream(flowie_supervisor_runtime_t *runtime, FILE *stream,
                                            int stdout_stream) {
  unsigned char buffer[FLOWIE_SUPERVISOR_IO_CHUNK];
  int rc;
  for (;;) {
    size_t count = 0;
    rc = stdout_stream
             ? flowie_supervisor_runtime_read_stdout(runtime, buffer, sizeof(buffer), &count)
             : flowie_supervisor_runtime_read_stderr(runtime, buffer, sizeof(buffer), &count);
    if (count > 0 && fwrite(buffer, 1u, count, stream) != count) return TURBO_EIO;
    if (rc == TURBO_EOF) return TURBO_OK;
    if (rc != TURBO_OK) return rc;
    if (count == 0) return TURBO_OK;
  }
}

int main(int argc, char **argv) {
  flowie_supervisor_runtime_config_t config = FLOWIE_SUPERVISOR_RUNTIME_CONFIG_INIT;
  flowie_supervisor_error_t error = FLOWIE_SUPERVISOR_ERROR_INIT;
  flowie_supervisor_runtime_t *runtime = NULL;
  turbo_process_result_t child = {TURBO_PROCESS_STARTING, -1, -1, 0, 0};
  char default_worker[TURBO_FS_MAX_PATH];
  int result = EXIT_FAILURE;
  int rc;

  for (int index = 1; index < argc; ++index) {
    if (strcmp(argv[index], "--check") == 0) {
      config.check_only = 1;
    } else if (strcmp(argv[index], "--require-security") == 0) {
      config.require_security = 1;
    } else if (strcmp(argv[index], "--profile") == 0) {
      if (++index >= argc || !argv[index][0]) {
        flowie_supervisor_usage(argv[0]);
        goto done;
      }
      config.profile = argv[index];
    } else if (strcmp(argv[index], "--control-config") == 0) {
      if (++index >= argc || !argv[index][0] || config.control_config_path) {
        flowie_supervisor_usage(argv[0]);
        goto done;
      }
      config.control_config_path = argv[index];
    } else if (strcmp(argv[index], "--worker") == 0) {
      if (++index >= argc || !argv[index][0]) {
        flowie_supervisor_usage(argv[0]);
        goto done;
      }
      config.worker_program = argv[index];
    } else if (strcmp(argv[index], "--capture-output") == 0) {
      if (++index >= argc ||
          flowie_supervisor_parse_size(argv[index], &config.max_output_bytes) != TURBO_OK) {
        flowie_supervisor_usage(argv[0]);
        goto done;
      }
      config.capture_output = 1;
    } else if (strcmp(argv[index], "--help") == 0 || strcmp(argv[index], "-h") == 0) {
      flowie_supervisor_usage(argv[0]);
      result = EXIT_SUCCESS;
      goto done;
    } else if (!config.config_path) {
      config.config_path = argv[index];
    } else if (!config.graph_path) {
      config.graph_path = argv[index];
    } else {
      flowie_supervisor_usage(argv[0]);
      goto done;
    }
  }
  if (!config.config_path || !config.graph_path) {
    flowie_supervisor_usage(argv[0]);
    goto done;
  }
  if (!config.worker_program) {
    rc = flowie_supervisor_default_worker(argv[0], default_worker, sizeof(default_worker));
    if (rc != TURBO_OK) {
      error.operation = "resolve worker path";
      error.status = rc;
      result = flowie_supervisor_report(&error);
      goto done;
    }
    config.worker_program = default_worker;
  }
  if (signal(SIGINT, flowie_supervisor_signal) == SIG_ERR ||
      signal(SIGTERM, flowie_supervisor_signal) == SIG_ERR) {
    error.operation = "install signal handlers";
    error.status = TURBO_EIO;
    result = flowie_supervisor_report(&error);
    goto done;
  }
  rc = flowie_supervisor_runtime_create(&config, &runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_supervisor_report(&error);
    goto done;
  }
  rc = flowie_supervisor_runtime_start(runtime, &error);
  if (rc != TURBO_OK) {
    result = flowie_supervisor_report(&error);
    goto done;
  }
  while (!flowie_supervisor_stop_requested) {
    rc = flowie_supervisor_runtime_wait_for(runtime, FLOWIE_SUPERVISOR_WAIT_INTERVAL_MS, &child,
                                            &error);
    if (rc == TURBO_OK) break;
    if (rc != TURBO_ETIMEDOUT) {
      result = flowie_supervisor_report(&error);
      goto done;
    }
  }
  if (flowie_supervisor_stop_requested) {
    rc = flowie_supervisor_runtime_stop(runtime, &child, &error);
    if (rc != TURBO_OK) {
      result = flowie_supervisor_report(&error);
      goto done;
    }
  }
  if (config.capture_output) {
    rc = flowie_supervisor_forward_stream(runtime, stdout, 1);
    if (rc == TURBO_OK) rc = flowie_supervisor_forward_stream(runtime, stderr, 0);
    if (rc != TURBO_OK) {
      error.operation = "forward worker output";
      error.status = rc;
      result = flowie_supervisor_report(&error);
      goto done;
    }
  }
  if (flowie_supervisor_stop_requested) {
    result = EXIT_SUCCESS;
  } else if (child.state == TURBO_PROCESS_EXITED && child.exit_code == 0) {
    result = EXIT_SUCCESS;
  } else {
    flowie_supervisor_report_child(&child);
    result = EXIT_FAILURE;
  }

done:
  flowie_supervisor_runtime_destroy(runtime);
  return result;
}
