#ifndef FLOWIE_RULE_INTERNAL_H
#define FLOWIE_RULE_INTERNAL_H

#include "flowie_mqtt_protocol.h"
#include "turbo_flow_policy.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Encode MQTT wire metadata into the serializable private message flags field. */
CXX_C_API int flowie_mqtt_message_flags_encode(flowie_mqtt_version_t version,
                                               uint8_t fixed_flags, uint32_t *flags_out);

/** Reserved pointer-free marker for a broker-generated Will traversing the graph. */
#define FLOWIE_MQTT_MESSAGE_BROKER_WILL UINT32_C(0x80000000)

/** Decode the MQTT version while ignoring reserved private marker bits. */
CXX_C_API int flowie_mqtt_message_flags_version(uint32_t flags,
                                                flowie_mqtt_version_t *version_out);

/** Immutable schema containing every MQTT PUBLISH fact provided by this module. */
CXX_C_API const turbo_flow_expr_schema_t *flowie_mqtt_rule_schema(void);

/**
 * Bind one branch-local typed projection after the message owns its wire payload.
 * `packet_hint` may borrow the pre-copy parser view; NULL reparses the owned payload.
 */
CXX_C_API int flowie_mqtt_rule_bind_projection(turbo_flow_msg_t *message,
                                               const flowie_mqtt_packet_view_t *packet_hint);

/** Read requested MQTT PUBLISH fields from a bound projection or an opaque Queue payload. */
CXX_C_API int flowie_mqtt_rule_facts_provider(const turbo_flow_msg_t *message,
                                              const turbo_flow_expr_schema_t *schema,
                                              const turbo_flow_expr_value_t **values_out,
                                              size_t *value_count_out, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_RULE_INTERNAL_H */
