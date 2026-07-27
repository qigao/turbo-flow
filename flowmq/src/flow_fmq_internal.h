#ifndef FLOW_FMQ_INTERNAL_H
#define FLOW_FMQ_INTERNAL_H

#include "turbo_flow_fmq.h"

int flow_fmq_app_create_endpoint_internal(
    const char *name, const turbo_flow_fmq_config_t *endpoint,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_coronet_execution_binding_t *execution,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out);

int flow_fmq_app_create_resolved_endpoint_internal(
    const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_fmq_app_options_t *options,
    const turbo_flow_fmq_security_binding_t *security, turbo_flow_fmq_app_t **out,
    turbo_flow_config_error_t *error);

#endif
