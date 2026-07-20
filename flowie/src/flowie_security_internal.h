#ifndef FLOWIE_SECURITY_INTERNAL_H
#define FLOWIE_SECURITY_INTERNAL_H

#include "flowie.h"

#include "turbo_str.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_ABI_V1 1u

/**
 * @internal Parser-proven MQTT resource borrowed only for one synchronous authorization call.
 * The resource must be the byte-for-byte tstr copy of a Topic Name or Topic Filter already
 * accepted by Flowie's MQTT parser.
 */
typedef struct flowie_mqtt_validated_security_context_s {
  flowie_mqtt_security_context_t public_context;
  uint32_t abi_version;
  flowie_mqtt_span_t resource;
  const void *provenance;
} flowie_mqtt_validated_security_context_t;

#define FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_INIT                                                \
  {{sizeof(flowie_mqtt_validated_security_context_t), FLOWIE_MQTT_SECURITY_TOPIC},                 \
   FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_ABI_V1,                                                  \
   {NULL, 0u},                                                                                     \
   NULL}

CXX_C_API int
flowie_mqtt_validated_security_context_init(flowie_mqtt_validated_security_context_t *out,
                                            flowie_mqtt_security_resource_kind_t kind,
                                            tstr_t parser_validated_resource);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_SECURITY_INTERNAL_H */
