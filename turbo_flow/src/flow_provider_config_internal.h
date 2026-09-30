#ifndef TURBO_FLOW_PROVIDER_CONFIG_INTERNAL_H
#define TURBO_FLOW_PROVIDER_CONFIG_INTERNAL_H

#include "flow_internal.h"

#include <cserde/reader.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flow_provider_config_reader_s {
  cserde_reader reader;
  const vec_t *literals;
  size_t token_index;
} flow_provider_config_reader_t;

int flow_provider_config_literals_init(vec_t *literals);
void flow_provider_config_literals_destroy(vec_t *literals);
int flow_provider_config_literals_copy(vec_t *destination, const vec_t *source);

int flow_provider_config_reader_init(
    flow_provider_config_reader_t *config_reader,
    const flow_stage_plan_impl_t *stage);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_CONFIG_INTERNAL_H */
