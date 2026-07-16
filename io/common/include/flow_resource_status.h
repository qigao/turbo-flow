#ifndef FLOW_RESOURCE_STATUS_H
#define FLOW_RESOURCE_STATUS_H

#include "flow_connection.h"

#ifdef __cplusplus
extern "C" {
#endif

int tf_connection_resource_metadata(const tf_connection_state_t *connection, const char *uid,
                                    const char *owner_name, turbo_flow_domain_t domain,
                                    turbo_flow_resource_metadata_t *out);
int tf_connection_status_document(const tf_connection_state_t *connection, const char *uid,
                                  const char *owner_name, turbo_flow_domain_t domain,
                                  const turbo_flow_resource_schema_t *schema,
                                  turbo_flow_resource_document_kind_t document_kind,
                                  turbo_flow_resource_document_t *out);

#ifdef __cplusplus
}
#endif

#endif
