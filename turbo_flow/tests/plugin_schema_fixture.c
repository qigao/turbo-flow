#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_operation.h"
#include <cmeta/type_select.h>

#include <string.h>

#ifndef FLOW_SCHEMA_FIXTURE_ID
#define FLOW_SCHEMA_FIXTURE_ID "fixture.schema.good"
#endif
#ifndef FLOW_SCHEMA_MODE
#define FLOW_SCHEMA_MODE 0
#endif
#ifndef FLOW_SCHEMA_ABI_MINOR
#define FLOW_SCHEMA_ABI_MINOR TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR
#endif
#ifndef FLOW_SCHEMA_ABI_MAJOR
#define FLOW_SCHEMA_ABI_MAJOR TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR
#endif
#ifndef FLOW_SCHEMA_STABLE_ID
#define FLOW_SCHEMA_STABLE_ID "fixture.schema.int"
#endif
#ifndef FLOW_SCHEMA_DESCRIPTOR_MINOR
#define FLOW_SCHEMA_DESCRIPTOR_MINOR TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR
#endif
#ifndef FLOW_SCHEMA_DESCRIPTOR_MAJOR
#define FLOW_SCHEMA_DESCRIPTOR_MAJOR TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR
#endif
#ifndef FLOW_SCHEMA_VERSION
#define FLOW_SCHEMA_VERSION 1u
#endif

enum { FLOW_SCHEMA_GOOD = 0, FLOW_SCHEMA_DUPLICATE = 1, FLOW_SCHEMA_BAD_VERSION = 2,
       FLOW_SCHEMA_BAD_DESCRIPTOR = 3, FLOW_SCHEMA_SWALLOW_ERROR = 4,
       FLOW_SCHEMA_BAD_ID = 5, FLOW_SCHEMA_LONG_ID = 6 };

typedef struct schema_fixture_s { const turbo_flow_plugin_host_v1_t *host; } schema_fixture_t;
static cmeta_type_desc fixture_int_type;
static cmeta_data_desc fixture_int_data;

static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  schema_fixture_t *fixture;
  if (!host || !out || !host->allocate || !host->deallocate) return SALTS_EINVAL;
  *out = NULL;
  fixture_int_type = *CMETA_TYPEOF(int);
  fixture_int_data = cmeta_data_int;
  fixture_int_data.stable_id = FLOW_SCHEMA_STABLE_ID;
#if FLOW_SCHEMA_MODE == 5
  fixture_int_data.stable_id = "fixture.schema/invalid";
#elif FLOW_SCHEMA_MODE == 6
  fixture_int_data.stable_id =
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
#endif
  fixture_int_data.display_name = "Fixture int";
  fixture_int_data.storage_type = &fixture_int_type;
  fixture = (schema_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  fixture->host = host;
  *out = fixture;
  return SALTS_OK;
}

static int fixture_register(void *plugin, const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_schema_v1_t schema = TURBO_FLOW_PLUGIN_SCHEMA_V1_INIT;
  int rc;
  if (!plugin || !registration || !registration->add_schema) return SALTS_EINVAL;
  schema.schema_version = FLOW_SCHEMA_MODE == FLOW_SCHEMA_BAD_VERSION ? 0u : FLOW_SCHEMA_VERSION;
  schema.abi_minor = FLOW_SCHEMA_DESCRIPTOR_MINOR;
  schema.abi_major = FLOW_SCHEMA_DESCRIPTOR_MAJOR;
  schema.data = &fixture_int_data;
#if FLOW_SCHEMA_MODE == 3
  fixture_int_data.storage_type = NULL;
#endif
  rc = registration->add_schema(registration->ctx, &schema);
#if FLOW_SCHEMA_MODE == 1
  if (rc == SALTS_OK) rc = registration->add_schema(registration->ctx, &schema);
  return rc;
#elif FLOW_SCHEMA_MODE == 4
  return SALTS_OK;
#else
  return rc;
#endif
}

static int fixture_quiesce(void *plugin, uint64_t timeout_ms) { (void)timeout_ms; return plugin ? SALTS_OK : SALTS_EINVAL; }
static int fixture_shutdown(void *plugin) { return plugin ? SALTS_OK : SALTS_EINVAL; }
static void fixture_destroy(void *plugin) {
  schema_fixture_t *fixture = (schema_fixture_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!fixture) return;
  host = fixture->host;
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t), FLOW_SCHEMA_ABI_MAJOR, FLOW_SCHEMA_ABI_MINOR,
    FLOW_SCHEMA_FIXTURE_ID, "1.0.0", TURBO_FLOW_PLUGIN_CAP_SCHEMA, fixture_load, fixture_register,
    fixture_quiesce, fixture_shutdown, fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) { return &fixture_api; }
