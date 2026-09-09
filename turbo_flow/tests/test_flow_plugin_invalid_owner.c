#include "plugin_generation_owner_fixture.h"
#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"

static const char owner_yaml[] = "version: 1\nadapters:\n  input.adapter:\n"
                                 "    kind: fixture.transactional.adapter\n    config: {}\n";
static const char owner_dsl[] =
    "source input adapter input.adapter\n"
    "stage output operation debug.log\nstage main {\n input -> output\n}\n";

typedef struct invalid_owner_test_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_plugin_result_domain_t *domain;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_t *flow;
  turbo_flow_plugin_generation_t *generation, *cleanup;
  generation_owner_observer_t *observer;
} invalid_owner_test_t;

static void owner_open(invalid_owner_test_t *t, int mode, const char *path) {
  turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  check_equal(turbo_flow_plugin_host_create(&hc, &t->host, &pe), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(t->host, path, &pe), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(t->host, &t->snapshot, &pe), SALTS_OK);
  check_equal(
      turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(t->snapshot, &catalog),
      SALTS_OK);
  t->observer = catalog.adapter_providers[0].ctx;
  t->observer->mode = mode;
  check_equal(turbo_flow_plugin_result_domain_create(t->snapshot, 1u, &t->domain, &pe), SALTS_OK);
  check_equal(
      turbo_flow_config_resolve_yaml(owner_yaml, sizeof(owner_yaml) - 1u, &t->resolved, &ce),
      SALTS_OK);
  t->flow = turbo_flow_create();
  check_not_null(t->flow);
  check_equal(turbo_flow_parse_string(t->flow, owner_dsl, sizeof(owner_dsl) - 1u), SALTS_OK);
}

suite("invalid Product owner DLL boundary") {
  it("destroys exact ABI owners with missing lifecycle callbacks only after Graph destruction") {
    const int modes[] = {OWNER_FIXTURE_NO_QUIESCE, OWNER_FIXTURE_NO_DRAIN,
                         OWNER_FIXTURE_NO_SHUTDOWN};
    for (size_t i = 0u; i < sizeof(modes) / sizeof(modes[0]); ++i) {
      invalid_owner_test_t t = {0};
      turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
      owner_open(&t, modes[i], FLOW_INVALID_OWNER_FIXTURE);
      check_equal(turbo_flow_plugin_generation_create(t.snapshot, t.resolved, &t.flow, &gc,
                                                      t.domain, &t.generation, &t.cleanup, &ce),
                  SALTS_EPROTO);
      check_null(t.flow);
      check_null(t.generation);
      check_null(t.cleanup);
      check_equal(t.observer->lifecycle_calls, 0u);
      check_equal(t.observer->destroys, 1u);
      check_equal(t.observer->graph_shutdowns, 1u);
      check_equal(t.observer->destroys_before_graph, 0u);
      check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_OK);
      turbo_flow_resolved_config_destroy(t.resolved);
      turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
      check_equal(turbo_flow_plugin_host_destroy(t.host, 0u, &pe), SALTS_OK);
    }
  }

  it("pins unknown ABI and unreleaseable owners without callbacks across repeated cleanup") {
    const int modes[] = {OWNER_FIXTURE_SHORT,        OWNER_FIXTURE_TINY,
                         OWNER_FIXTURE_LONG,         OWNER_FIXTURE_OLD_MAJOR,
                         OWNER_FIXTURE_FUTURE_MAJOR, OWNER_FIXTURE_FUTURE_MINOR,
                         OWNER_FIXTURE_NO_DESTROY,   OWNER_FIXTURE_NO_CONTEXT,
                         OWNER_FIXTURE_NORMAL,       OWNER_FIXTURE_NORMAL};
    /* CTest runs this file in its own process. Contract-violating objects intentionally stay
       pinned until process exit; no public force-unload or repair contract exists. */
    for (size_t i = 0u; i < sizeof(modes) / sizeof(modes[0]); ++i) {
      invalid_owner_test_t t = {0};
      turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_result_domain_snapshot_v3_t domain_state;
      const char *path = i == 8u   ? FLOW_INVALID_OWNER_LEGACY
                         : i == 9u ? FLOW_INVALID_OWNER_SMALL_POLL
                                   : FLOW_INVALID_OWNER_FIXTURE;
      int expected = modes[i] == OWNER_FIXTURE_NO_DESTROY || modes[i] == OWNER_FIXTURE_NO_CONTEXT
                         ? SALTS_EPROTO
                         : SALTS_EINVAL;
      owner_open(&t, modes[i], path);
      check_equal(turbo_flow_plugin_generation_create(t.snapshot, t.resolved, &t.flow, &gc,
                                                      t.domain, &t.generation, &t.cleanup, &ce),
                  expected);
      check_null(t.flow);
      check_null(t.generation);
      check_not_null(t.cleanup);
      check_equal(ce.status, expected);
      turbo_flow_resolved_config_destroy(t.resolved);
      turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
      for (size_t retry = 0u; retry < 2u; ++retry) {
        check_equal(turbo_flow_plugin_generation_state(t.cleanup),
                    TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP);
        check_null(turbo_flow_plugin_generation_flow(t.cleanup));
        check_equal(turbo_flow_plugin_generation_poll(t.cleanup, 0u, &cleanup_error), SALTS_EBUSY);
        check_equal(turbo_flow_plugin_generation_cleanup_error(t.cleanup, &cleanup_error),
                    SALTS_OK);
        check_equal(cleanup_error.status, SALTS_EINVAL);
        check_equal(turbo_flow_plugin_generation_destroy(t.cleanup, 0u, &cleanup_error),
                    SALTS_EINVAL);
        check_equal(t.observer->lifecycle_calls, 0u);
        check_equal(t.observer->destroys, 0u);
        check_equal(t.observer->graph_shutdowns, 0u);
        turbo_flow_plugin_result_domain_snapshot_v3_init(&domain_state);
        check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &domain_state), SALTS_OK);
        check_equal(domain_state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
        check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_EBUSY);
        check_equal(turbo_flow_plugin_host_destroy(t.host, 0u, &pe), SALTS_EBUSY);
      }
    }
  }
}
