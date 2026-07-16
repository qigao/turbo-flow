#ifndef FLOW_PGSQL_INTERNAL_H
#define FLOW_PGSQL_INTERNAL_H

#include "libpq-fe.h"
#include "turbo_flow_pgsql.h"

int flow_pgsql_result_to_message(PGresult *result, const char *statement_name, size_t max_rows,
                                 size_t max_result_bytes,
                                 turbo_flow_pgsql_result_format_t result_format,
                                 const turbo_flow_content_descriptor_t *rowset_descriptor,
                                 const turbo_flow_content_descriptor_t *command_descriptor,
                                 turbo_flow_msg_t *msg);
int flow_pgsql_result_bind_projection(PGresult *result, turbo_flow_msg_t *msg,
                                      const turbo_flow_schema_registry_t *registry,
                                      const char *schema_name, const char *type_name,
                                      uint32_t schema_version,
                                      const turbo_flow_pgsql_row_mapper_t *mapper);

#endif /* FLOW_PGSQL_INTERNAL_H */
