#ifndef TURBO_FLOW_TEST_SALTS_RESOURCE_FIXTURE_H
#define TURBO_FLOW_TEST_SALTS_RESOURCE_FIXTURE_H

#include <cmeta/interface.h>
#include <stdint.h>

#define FLOW_TEST_RESOURCE_CONTRACT_ID "test.turboflow.resource"
#define FLOW_TEST_RESOURCE_IDENTITY "db_main"
enum {
  FLOW_TEST_RESOURCE_CONTRACT_VERSION = 1u,
  FLOW_TEST_RESOURCE_CAP_READ = UINT64_C(1) << 0
};

#define FLOW_TEST_RESOURCE_METHODS(X, I)   X(I, R0, int, ping, _)

CMETA_INTERFACE(flow_test_resource, FLOW_TEST_RESOURCE_METHODS);

#endif
