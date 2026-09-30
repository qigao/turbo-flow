#include "tinytest.h"
#include "turbo_flow_diagnostic.h"

#include <string.h>

spec("TurboFlow diagnostic ABI") {
  it("initializes one generic control-plane diagnostic") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(error.size, sizeof(error));
    check_equal(error.status, SALTS_OK);
    check_equal(error.path[0], '\0');
    check_equal(error.message[0], '\0');
    check_equal(sizeof(error.path), (size_t)TURBO_FLOW_DIAGNOSTIC_PATH_MAX + 1u);
    check_equal(sizeof(error.message), (size_t)TURBO_FLOW_DIAGNOSTIC_MESSAGE_MAX + 1u);
  }
}
