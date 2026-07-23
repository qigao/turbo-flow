#ifndef TURBO_FLOW_REDIS_INTERNAL_H
#define TURBO_FLOW_REDIS_INTERNAL_H

#include "redis_client.h"
#include "turbo_flow.h"

typedef struct flow_redis_store_client_s flow_redis_store_client_t;

typedef struct flow_redis_store_client_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
} flow_redis_store_client_config_t;

typedef int (*flow_redis_store_reply_fn)(void *ctx, const redis_reply_t *reply);

int flow_redis_store_client_create(const flow_redis_store_client_config_t *config,
                                   flow_redis_store_client_t **out);
void flow_redis_store_client_destroy(flow_redis_store_client_t *client);
int flow_redis_store_command(flow_redis_store_client_t *client, int argc, const char **argv,
                             const size_t *lengths, flow_redis_store_reply_fn apply, void *ctx);

/*
 * Internal point lookup for the Redis record provider. The visited record is borrowed and remains
 * valid only for the callback. The record-store mutex serializes this command with scan/commit.
 */
int flow_redis_record_store_get(turbo_flow_record_store_t *store, const uint8_t *key,
                                size_t key_size, turbo_flow_record_visit_fn visit, void *visit_ctx);

#endif
