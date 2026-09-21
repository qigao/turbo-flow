#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_materializer.h"

#include <cmeta/type_select.h>
#include <string.h>

#ifndef FLOW_MATERIALIZER_FIXTURE_ID
#define FLOW_MATERIALIZER_FIXTURE_ID "fixture.materializer.good"
#endif
#ifndef FLOW_MATERIALIZER_SCHEMA_ID
#define FLOW_MATERIALIZER_SCHEMA_ID "fixture.materializer.int"
#endif
#ifndef FLOW_MATERIALIZER_SCHEMA_VERSION
#define FLOW_MATERIALIZER_SCHEMA_VERSION 1u
#endif
#ifndef FLOW_MATERIALIZER_TYPE_NAME
#define FLOW_MATERIALIZER_TYPE_NAME "FixtureInt"
#endif
#ifndef FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID
#define FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID 15901u
#endif
#ifndef FLOW_MATERIALIZER_MODE
#define FLOW_MATERIALIZER_MODE 0
#endif
#ifndef FLOW_MATERIALIZER_ABI_MAJOR
#define FLOW_MATERIALIZER_ABI_MAJOR TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR
#endif
#ifndef FLOW_MATERIALIZER_ABI_MINOR
#define FLOW_MATERIALIZER_ABI_MINOR TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR
#endif
#ifndef FLOW_MATERIALIZER_DESCRIPTOR_MAJOR
#define FLOW_MATERIALIZER_DESCRIPTOR_MAJOR TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR
#endif
#ifndef FLOW_MATERIALIZER_DESCRIPTOR_MINOR
#define FLOW_MATERIALIZER_DESCRIPTOR_MINOR TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR
#endif

enum {
  FLOW_MATERIALIZER_GOOD = 0,
  FLOW_MATERIALIZER_DUPLICATE = 1,
  FLOW_MATERIALIZER_BAD_SCHEMA_VERSION = 2,
  FLOW_MATERIALIZER_BAD_NATIVE_SIZE = 3,
  FLOW_MATERIALIZER_BAD_MAX_ENCODED = 4,
  FLOW_MATERIALIZER_MISSING_CALLBACK = 5,
  FLOW_MATERIALIZER_SWALLOW_ERROR = 6
};

typedef struct materializer_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
} materializer_fixture_t;

static cmeta_type_desc fixture_int_type;
static cmeta_data_desc fixture_int_data;

static int fixture_materialize(
    void *ctx, const turbo_flow_plugin_materializer_input_v1_t *input,
    void *native_out, size_t native_capacity) {
  (void)ctx;
  if (!input || input->size != sizeof(*input) ||
      input->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      input->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !input->data || input->data_size != sizeof(int) ||
      !native_out || native_capacity != sizeof(int))
    return SALTS_EINVAL;
  memcpy(native_out, input->data, sizeof(int));
  return SALTS_OK;
}

static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  materializer_fixture_t *fixture;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !out || !host->allocate || !host->deallocate)
    return SALTS_EINVAL;
  *out = NULL;
  fixture_int_type = *CMETA_TYPEOF(int);
  fixture_int_data = cmeta_data_int;
  fixture_int_data.stable_id = FLOW_MATERIALIZER_SCHEMA_ID;
  fixture_int_data.display_name = "Fixture materialized int";
  fixture_int_data.storage_type = &fixture_int_type;
  fixture = (materializer_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  fixture->host = host;
  *out = fixture;
  return SALTS_OK;
}

static int fixture_register(
    void *plugin, const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_materializer_v1_t materializer =
      TURBO_FLOW_PLUGIN_MATERIALIZER_V1_INIT;
  int rc;
  if (!plugin || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_materializer)
    return SALTS_EINVAL;

  materializer.abi_major = FLOW_MATERIALIZER_DESCRIPTOR_MAJOR;
  materializer.abi_minor = FLOW_MATERIALIZER_DESCRIPTOR_MINOR;
  materializer.schema.domain = TURBO_FLOW_DOMAIN_DATA;
  materializer.schema.encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
  materializer.schema.schema_name = FLOW_MATERIALIZER_SCHEMA_ID;
  materializer.schema.type_name = FLOW_MATERIALIZER_TYPE_NAME;
  materializer.schema.projection_type = fixture_int_type.name;
  materializer.schema.schema_id = FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID;
  materializer.schema.schema_version =
      FLOW_MATERIALIZER_MODE == FLOW_MATERIALIZER_BAD_SCHEMA_VERSION
          ? 0u
          : FLOW_MATERIALIZER_SCHEMA_VERSION;
  materializer.data = &fixture_int_data;
  materializer.max_encoded_bytes =
      FLOW_MATERIALIZER_MODE == FLOW_MATERIALIZER_BAD_MAX_ENCODED
          ? 0u
          : sizeof(int);
  materializer.native_bytes =
      FLOW_MATERIALIZER_MODE == FLOW_MATERIALIZER_BAD_NATIVE_SIZE
          ? sizeof(int) + 1u
          : sizeof(int);
  materializer.threading = TURBO_FLOW_PLUGIN_MATERIALIZER_THREAD_SAFE;
  materializer.ownership = TURBO_FLOW_PLUGIN_MATERIALIZER_CALLER_BUFFER;
  materializer.materialize =
      FLOW_MATERIALIZER_MODE == FLOW_MATERIALIZER_MISSING_CALLBACK
          ? NULL
          : fixture_materialize;

  rc = registration->add_materializer(registration->ctx, &materializer);
  if (FLOW_MATERIALIZER_MODE == FLOW_MATERIALIZER_DUPLICATE && rc == SALTS_OK)
    rc = registration->add_materializer(registration->ctx, &materializer);
  if (FLOW_MATERIALIZER_MODE == FLOW_MATERIALIZER_SWALLOW_ERROR)
    return SALTS_OK;
  return rc;
}

static int fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static int fixture_shutdown(void *plugin) {
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static void fixture_destroy(void *plugin) {
  materializer_fixture_t *fixture = (materializer_fixture_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!fixture) return;
  host = fixture->host;
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    FLOW_MATERIALIZER_ABI_MAJOR,
    FLOW_MATERIALIZER_ABI_MINOR,
    FLOW_MATERIALIZER_FIXTURE_ID,
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_MATERIALIZER,
    fixture_load,
    fixture_register,
    fixture_quiesce,
    fixture_shutdown,
    fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *
turbo_flow_plugin_get_api(void) {
  return &fixture_api;
}
