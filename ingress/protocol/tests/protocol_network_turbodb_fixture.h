#ifndef PROTOCOL_NETWORK_TURBODB_FIXTURE_H
#define PROTOCOL_NETWORK_TURBODB_FIXTURE_H

#include "turbo_flow_turbodb.h"

#include <orm.h>

#include <stdint.h>

typedef struct protocol_network_turbodb_fixture_s {
  char *path;
  orm_option_t filename;
  orm_config_t database;
} protocol_network_turbodb_fixture_t;

int protocol_network_turbodb_fixture_init(protocol_network_turbodb_fixture_t *fixture);
int protocol_network_turbodb_fixture_provision(protocol_network_turbodb_fixture_t *fixture);
int protocol_network_turbodb_fixture_open(protocol_network_turbodb_fixture_t *fixture,
                                          turbo_flow_inbox_t *out);
int protocol_network_turbodb_fixture_writer_lock(protocol_network_turbodb_fixture_t *fixture,
                                                 orm_connection_t **connection_out,
                                                 orm_transaction_t **transaction_out);
int protocol_network_turbodb_fixture_writer_unlock(orm_connection_t *connection,
                                                   orm_transaction_t *transaction);
int protocol_network_turbodb_fixture_drop_records(protocol_network_turbodb_fixture_t *fixture);
int protocol_network_turbodb_fixture_pending_count(protocol_network_turbodb_fixture_t *fixture,
                                                   int64_t *out);
void protocol_network_turbodb_fixture_destroy(protocol_network_turbodb_fixture_t *fixture);

#endif /* PROTOCOL_NETWORK_TURBODB_FIXTURE_H */
