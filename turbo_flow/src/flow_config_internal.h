#ifndef TURBO_FLOW_CONFIG_INTERNAL_H
#define TURBO_FLOW_CONFIG_INTERNAL_H

#include "turbo_flow_resolved_config.h"

#include "turbo_parser.h"

struct turbo_flow_resolved_config_s {
  turbo_json_doc_t *document;
  char *json;
  size_t json_len;
};

#endif /* TURBO_FLOW_CONFIG_INTERNAL_H */
