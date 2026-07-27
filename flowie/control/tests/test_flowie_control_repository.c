#include "flowie_control_repository_contract.h"
#include "flowie_control_repository_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdlib.h>

typedef struct repository_fixture_s {
  char *path;
  flowie_control_store_t *store;
  const flowie_control_repository_t *repository;
} repository_fixture_t;

static repository_fixture_t repository_fixture_open(void) {
  repository_fixture_t fixture = {0};
  flowie_control_store_config_t config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  fixture.path = tt_make_temp_file("flowie-control-repository", ".sqlite3");
  check_not_null(fixture.path);
  config.database_path = fixture.path;
  check_int_eq(flowie_control_store_open(&config, &fixture.store), TURBO_OK);
  fixture.repository = flowie_control_store_repository(fixture.store);
  check_not_null(fixture.repository);
  return fixture;
}

static void repository_fixture_close(repository_fixture_t *fixture) {
  flowie_control_store_destroy(fixture->store);
  check_int_eq(tt_remove_file(fixture->path), 0);
  free(fixture->path);
  *fixture = (repository_fixture_t){0};
}

spec("Flowie control repository provider contract") {
  it("rejects incomplete versions, capabilities, and operation tables") {
    repository_fixture_t fixture = repository_fixture_open();
    flowie_control_repository_t candidate = *fixture.repository;
    flowie_control_repository_auth_ops_t auth = *candidate.auth;
    flowie_control_repository_policy_ops_t policy = *candidate.policy;

    check_int_eq(flowie_control_repository_validate(fixture.repository), TURBO_OK);

    candidate.version = FLOWIE_CONTROL_REPOSITORY_VERSION + 1u;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);
    candidate = *fixture.repository;
    candidate.capabilities &= ~FLOWIE_CONTROL_REPOSITORY_ATOMIC_COMMANDS;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);
    candidate = *fixture.repository;
    candidate.policy = NULL;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);
    candidate = *fixture.repository;
    auth.principal_snapshot = NULL;
    candidate.auth = &auth;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);
    candidate = *fixture.repository;
    auth = *candidate.auth;
    auth.external_principal_snapshot = NULL;
    candidate.auth = &auth;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);
    candidate = *fixture.repository;
    policy = *candidate.policy;
    policy.bundle_load = NULL;
    candidate.policy = &policy;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);
    candidate = *fixture.repository;
    policy = *candidate.policy;
    policy.bundle_release = NULL;
    candidate.policy = &policy;
    check_int_eq(flowie_control_repository_validate(&candidate), TURBO_EINVAL);

    repository_fixture_close(&fixture);
  }

  it("preserves revision, audit, auth snapshot, and conflict semantics through the port") {
    repository_fixture_t fixture = repository_fixture_open();
    flowie_control_repository_basic_contract_run(fixture.repository);
    repository_fixture_close(&fixture);
  }
}
