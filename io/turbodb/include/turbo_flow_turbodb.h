#ifndef TURBO_FLOW_TURBODB_H
#define TURBO_FLOW_TURBODB_H

#include "turbo_flow.h"

#include <orm.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_TURBODB_API_VERSION UINT32_C(1)

/**
 * Message metadata and trusted schema for one typed TurboDb Publisher.
 *
 * The projection schema, its strings, and the CMeta descriptor named by
 * projection_type are borrowed and must outlive the Publisher and every
 * emitted message or clone. The schema must describe DATA-domain values and
 * projection_type must equal the wrapped Publisher's CMeta type name.
 *
 * @code{.c}
 * static int open_rows(orm_query_t *query, const orm_flow_config_t *flow,
 *                      const turbo_flow_data_schema_t *schema,
 *                      orm_error_t *error, cflow_publisher *out) {
 *   turbo_flow_turbodb_source_config_t source =
 *       turbo_flow_turbodb_source_config_default();
 *   source.projection_schema = schema;
 *   return turbo_flow_turbodb_query_open(query, flow, &source, out, error);
 * }
 * @endcode
 */
typedef struct turbo_flow_turbodb_source_config_s {
  size_t size;
  uint32_t version;
  const turbo_flow_data_schema_t *projection_schema;
  uint64_t first_message_id;
  uint32_t message_type;
  uint32_t message_flags;
} turbo_flow_turbodb_source_config_t;

/**
 * Return a complete versioned configuration with message IDs starting at 1.
 * projection_schema remains NULL and must be set before open/wrap.
 */
TURBO_FLOW_C_API turbo_flow_turbodb_source_config_t turbo_flow_turbodb_source_config_default(void);

/**
 * Move a typed, constructing CFlow Publisher into a TurboFlow message Publisher.
 *
 * On success typed_publisher is cleared and message_publisher owns the adapter.
 * A validation or allocation failure leaves typed_publisher live and
 * message_publisher empty. Each row is owned by its emitted message projection.
 * WAIT, wakers, cancellation, and terminal state are forwarded without polling
 * or materializing the full result set.
 *
 * @param typed_publisher Live constructing Publisher; moved only on success.
 * @param config Borrowed, validated source configuration.
 * @param message_publisher Zero-initialized output Publisher.
 * @return SALTS_OK, SALTS_EINVAL for an invalid contract, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_publisher_wrap(cflow_publisher *typed_publisher,
                                  const turbo_flow_turbodb_source_config_t *config,
                                  cflow_publisher *message_publisher);

/**
 * Open a native ORM row Publisher and move it into the message adapter.
 *
 * query, its connection, orm_config->row_shape, reachable CMeta metadata, and
 * source_config remain borrowed for the returned Publisher/message lifetimes.
 * Adapter validation does not modify orm_error; an ORM open failure does.
 *
 * @param query Live ORM query.
 * @param orm_config Borrowed native flow configuration and row shape.
 * @param source_config Borrowed message/projection configuration.
 * @param message_publisher Zero-initialized output Publisher.
 * @param orm_error Caller-initialized detailed ORM error storage.
 * @return SALTS_OK; SALTS_EINVAL/SALTS_ENOMEM for adapter failures; or a
 * mapped Salts category for the detailed ORM error.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_query_open(orm_query_t *query, const orm_flow_config_t *orm_config,
                              const turbo_flow_turbodb_source_config_t *source_config,
                              cflow_publisher *message_publisher, orm_error_t *orm_error);

/**
 * Transaction-scoped row form with the same errors and ownership as query_open.
 * transaction must also outlive the returned Publisher.
 */
TURBO_FLOW_C_API int turbo_flow_turbodb_query_open_in_transaction(
    orm_query_t *query, orm_transaction_t *transaction, const orm_flow_config_t *orm_config,
    const turbo_flow_turbodb_source_config_t *source_config, cflow_publisher *message_publisher,
    orm_error_t *orm_error);

/**
 * Open a native ORM command-result Publisher and move it into the adapter.
 * Command execution remains deferred until downstream demand resumes it.
 * query/source_config stay borrowed; orm_error receives ORM open diagnostics.
 *
 * @return SALTS_OK; SALTS_EINVAL/SALTS_ENOMEM for adapter failures; or a
 * mapped Salts category for the detailed ORM error.
 */
TURBO_FLOW_C_API int
turbo_flow_turbodb_command_open(orm_query_t *query,
                                const turbo_flow_turbodb_source_config_t *source_config,
                                cflow_publisher *message_publisher, orm_error_t *orm_error);

/**
 * Transaction-scoped command form with the same errors and ownership as
 * command_open. transaction must outlive the returned Publisher.
 */
TURBO_FLOW_C_API int turbo_flow_turbodb_command_open_in_transaction(
    orm_query_t *query, orm_transaction_t *transaction,
    const turbo_flow_turbodb_source_config_t *source_config, cflow_publisher *message_publisher,
    orm_error_t *orm_error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_TURBODB_H */
