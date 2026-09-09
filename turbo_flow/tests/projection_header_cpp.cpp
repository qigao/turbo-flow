#include "turbo_flow_projection.h"
#include <new>

namespace {
constexpr int expected_value = 53;
constexpr size_t result_capacity = 2;
struct Context {};
const turbo_flow_data_schema_t schema = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
    "cpp.retained", "Integer", "cpp.int", 1u, 1u, nullptr};
int clone_value(const void *value, void *, void **out) {
  *out = new (std::nothrow) int(*static_cast<const int *>(value));
  return *out ? SALTS_OK : SALTS_ENOMEM;
}
void destroy_value(void *value, void *) {
  delete static_cast<int *>(value);
}
int release_context(void *ctx) {
  delete static_cast<Context *>(ctx);
  return SALTS_OK;
}
}

extern "C" int flow_projection_cpp_example(void) {
  turbo_flow_result_memory_requirements_t requirements;
  turbo_flow_result_memory_requirements_init(&requirements);
  if (turbo_flow_result_memory_requirements(result_capacity, sizeof(int), &requirements) != SALTS_OK)
    return SALTS_EPROTO;
  turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
  turbo_flow_projection_owner_t *owner = nullptr;
  turbo_flow_msg_t source, copy;
  auto *ctx = new (std::nothrow) Context;
  auto *value = new (std::nothrow) int(expected_value);
  if (!ctx || !value) {
    delete ctx;
    delete value;
    return SALTS_ENOMEM;
  }
  config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                 TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  config.capacity = result_capacity;
  config.max_result_bytes = sizeof(int);
  config.max_retained_bytes = result_capacity * sizeof(int);
  config.schema = &schema;
  config.clone = clone_value;
  config.destroy = destroy_value;
  config.ctx = ctx;
  config.release_context = release_context;
  int rc = turbo_flow_projection_owner_create(&config, &owner);
  if (rc != SALTS_OK) {
    delete ctx;
    delete value;
    return rc;
  }
  turbo_flow_msg_init(&source);
  turbo_flow_msg_init(&copy);
  rc = turbo_flow_msg_bind_retained_projection(&source, owner, value);
  if (rc != SALTS_OK) delete value;
  if (rc == SALTS_OK) rc = turbo_flow_msg_clone(&copy, &source);
  if (rc == SALTS_OK) rc = turbo_flow_projection_owner_snapshot(owner, &state);
  if (rc == SALTS_OK && (state.outstanding != result_capacity ||
      *static_cast<const int *>(turbo_flow_msg_projection(&copy, nullptr)) != expected_value))
    rc = SALTS_EPROTO;
  turbo_flow_msg_cleanup(&source);
  turbo_flow_msg_cleanup(&copy);
  const int stop_rc = turbo_flow_projection_owner_stop(owner);
  if (rc == SALTS_OK) rc = stop_rc;
  const int destroy_rc = turbo_flow_projection_owner_destroy(owner);
  return rc == SALTS_OK ? destroy_rc : rc;
}
