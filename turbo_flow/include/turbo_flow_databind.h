#ifndef TURBO_FLOW_DATABIND_H
#define TURBO_FLOW_DATABIND_H

#include "turbo_flow_export.h"

#include <data_bind_flowmq_plan.h>
#include <data_bind_socket_plan.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_s turbo_flow_t;

/**
 * Bind one generated DataBind Socket Channel projection to an already parsed
 * TurboFlow Source stage.
 *
 * The generated plan and all metadata reachable from its native-binding
 * resolver must remain valid until the Flow is reset or destroyed. Official
 * salts_idl_target() generated plans satisfy this process/static lifetime.
 *
 * The resolver is invoked exactly once by this control-plane call. Compilation
 * and message execution use the retained native metadata and do not perform IDL,
 * schema, transport-kind, or resolver lookup.
 */
TURBO_FLOW_C_API int turbo_flow_product_bind_databind_socket_source(
    turbo_flow_t *flow, const char *stage_name,
    const DataBindSocketPlan *plan);

/** Same contract as turbo_flow_product_bind_databind_socket_source for FlowMQ. */
TURBO_FLOW_C_API int turbo_flow_product_bind_databind_flowmq_source(
    turbo_flow_t *flow, const char *stage_name,
    const DataBindFlowMQChannelPlan *plan);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DATABIND_H */
