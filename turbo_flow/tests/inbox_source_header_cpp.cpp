#include "turbo_flow_inbox_source.h"

extern "C" int flow_inbox_source_cpp_header_probe(void) {
  turbo_flow_inbox_source_config_t config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
  turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
  return config.version == TURBO_FLOW_INBOX_SOURCE_API_VERSION &&
                 result.version == TURBO_FLOW_INBOX_SOURCE_API_VERSION &&
                 result.state == TURBO_FLOW_INBOX_SOURCE_EMPTY
             ? 0
             : 1;
}
