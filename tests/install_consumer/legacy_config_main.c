#include <stddef.h>
#include <stdio.h>

#if defined(_WIN32)
  #define LEGACY_IMPORT __declspec(dllimport)
#else
  #define LEGACY_IMPORT
#endif

typedef struct turbo_flow_resolved_config_s turbo_flow_resolved_config_t;
typedef struct legacy_config_error_s {
  size_t size;
  int status;
  char path[256];
  char message[256];
} legacy_config_error_t;

LEGACY_IMPORT int turbo_flow_config_resolve_yaml(const char *yaml, size_t yaml_len,
                                                 turbo_flow_resolved_config_t **out,
                                                 legacy_config_error_t *error);
LEGACY_IMPORT void turbo_flow_resolved_config_destroy(turbo_flow_resolved_config_t *config);
LEGACY_IMPORT const char *turbo_flow_resolved_config_json(
    const turbo_flow_resolved_config_t *config, size_t *json_len);

int main(void) {
  static const char yaml[] = "version: 1\nadapters: {}\n";
  legacy_config_error_t error = {sizeof(error), 0, {0}, {0}};
  turbo_flow_resolved_config_t *config = NULL;
  size_t json_len = 0u;
  const char *json;
  if (turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error) != 0 || !config) {
    fprintf(stderr, "resolve failed: status=%d path=%s message=%s\n", error.status, error.path,
            error.message);
    return 1;
  }
  json = turbo_flow_resolved_config_json(config, &json_len);
  if (!json || json_len == 0u) {
    fprintf(stderr, "resolved JSON was empty\n");
    turbo_flow_resolved_config_destroy(config);
    return 1;
  }
  turbo_flow_resolved_config_destroy(config);
  return 0;
}
