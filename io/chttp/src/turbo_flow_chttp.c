#include "turbo_flow_chttp.h"

#include <salts/clock.h>
#include <salts/thread.h>

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum turbo_flow_chttp_slot_state_e {
  TURBO_FLOW_CHTTP_SLOT_FREE = 0,
  TURBO_FLOW_CHTTP_SLOT_QUEUED,
  TURBO_FLOW_CHTTP_SLOT_ACTIVE,
  TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT,
  TURBO_FLOW_CHTTP_SLOT_COMPLETING
} turbo_flow_chttp_slot_state_t;

typedef struct turbo_flow_chttp_response_storage_s {
  turbo_flow_chttp_response_context_t public_context;
  size_t reason_offset;
  size_t reason_size;
  size_t headers_offset;
} turbo_flow_chttp_response_storage_t;

typedef struct turbo_flow_chttp_header_storage_s {
  size_t name_offset;
  size_t name_size;
  size_t value_offset;
  size_t value_size;
} turbo_flow_chttp_header_storage_t;

typedef struct turbo_flow_chttp_slot_s {
  struct turbo_flow_chttp_client_s *owner;
  turbo_flow_chttp_slot_state_t state;
  turbo_flow_async_emit_claim_t claim;
  chttp_request request;
  uint32_t attempts;
  uint64_t deadline_ms;
  uint64_t retry_at_ms;
  int cancel_requested;
  int cancel_status;
} turbo_flow_chttp_slot_t;

struct turbo_flow_chttp_client_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  chttp_client_config config;
  tstr connection_uri;
  tstr authority;
  tstr target;
  chttp_method method;
  chttp_header *headers;
  size_t header_count;
  const chttp_tls_profile *tls;
  chttp_protocol protocol;
  uint32_t overall_timeout_ms;
  uint32_t max_attempts;
  uint32_t retry_delay_ms;
  uint32_t stop_timeout_ms;
  int idempotent;
  chttp_async_client http;
  turbo_flow_chttp_slot_t *slots;
  size_t slot_count;
  salts_mutex_t mutex;
  salts_cond_t owner_cond;
  int owner_active;
  turbo_flow_chttp_client_state_t state;
  uint64_t submitted_requests;
  uint64_t completed_requests;
  uint64_t retried_requests;
  uint64_t canceled_requests;
  uint64_t response_bytes;
  int last_status;
  int last_native_status;
};

static int chttp_adapter_add_size(size_t *total, size_t value) {
  if (!total || value > SIZE_MAX - *total) return SALTS_ERANGE;
  *total += value;
  return SALTS_OK;
}

static int chttp_adapter_method_valid(chttp_method method) {
  return method >= CHTTP_METHOD_GET && method <= CHTTP_METHOD_PATCH;
}

static int chttp_adapter_method_idempotent(const turbo_flow_chttp_client_t *client) {
  return client->idempotent || client->method == CHTTP_METHOD_GET ||
         client->method == CHTTP_METHOD_HEAD || client->method == CHTTP_METHOD_PUT ||
         client->method == CHTTP_METHOD_DELETE;
}

static int chttp_adapter_retryable(int status) {
  switch (status) {
  case SALTS_ECONNABORTED:
  case SALTS_ECONNREFUSED:
  case SALTS_ECONNRESET:
  case SALTS_EHOSTUNREACH:
  case SALTS_EINTR:
  case SALTS_EIO:
  case SALTS_ENETDOWN:
  case SALTS_ENETUNREACH:
  case SALTS_ENOTCONN:
  case SALTS_EPIPE:
  case SALTS_ETIMEDOUT:
    return 1;
  default:
    return 0;
  }
}

static int chttp_adapter_uri_valid(const turbo_flow_chttp_client_config_t *config) {
  const int is_tcp = strncmp(config->connection_uri, "tcp://", 6u) == 0;
  const int is_tls = strncmp(config->connection_uri, "tls://", 6u) == 0;
  const int is_pipe = strncmp(config->connection_uri, "pipe://", 7u) == 0;
  if (!is_tcp && !is_tls && !is_pipe) return 0;
  if ((config->tls != NULL) != is_tls) return 0;
  if (config->protocol == CHTTP_HTTP_2 && is_pipe) return 0;
  return 1;
}

static void chttp_adapter_counter_increment(uint64_t *counter) {
  if (counter && *counter != UINT64_MAX) ++*counter;
}

static uint64_t chttp_adapter_deadline_after(uint64_t now, uint32_t delay_ms) {
  return delay_ms > UINT64_MAX - now ? UINT64_MAX : now + delay_ms;
}

static void chttp_adapter_client_cleanup(turbo_flow_chttp_client_t *client) {
  size_t index;
  if (!client) return;
  for (index = 0u; index < client->header_count; ++index) {
    tstr name = (tstr)client->headers[index].name;
    tstr value = (tstr)client->headers[index].value;
    tstr_freep(&name);
    tstr_freep(&value);
  }
  free(client->headers);
  free(client->slots);
  tstr_freep(&client->adapter_name);
  tstr_freep(&client->connection_uri);
  tstr_freep(&client->authority);
  tstr_freep(&client->target);
}

static int chttp_adapter_copy_headers(turbo_flow_chttp_client_t *client,
                                      const chttp_header *headers, size_t header_count) {
  size_t index;
  if (header_count == 0u) return SALTS_OK;
  client->headers = (chttp_header *)calloc(header_count, sizeof(*client->headers));
  if (!client->headers) return SALTS_ENOMEM;
  client->header_count = header_count;
  for (index = 0u; index < header_count; ++index) {
    if (!headers[index].name || !headers[index].value || headers[index].name[0] == '\0') {
      return SALTS_EINVAL;
    }
    client->headers[index].name = tstr_dup(headers[index].name);
    client->headers[index].value = tstr_dup(headers[index].value);
    if (!client->headers[index].name || !client->headers[index].value) return SALTS_ENOMEM;
  }
  return SALTS_OK;
}

static int chttp_adapter_response_size(const chttp_response_view *response, size_t *out_size) {
  size_t index;
  size_t size = sizeof(turbo_flow_chttp_response_storage_t);
  if (!response || !out_size || (response->body_size != 0u && !response->body) ||
      (response->header_count != 0u && !response->headers))
    return SALTS_EINVAL;
  if (response->header_count > (SIZE_MAX - size) / sizeof(turbo_flow_chttp_header_storage_t))
    return SALTS_ERANGE;
  size += response->header_count * sizeof(turbo_flow_chttp_header_storage_t);
  if (chttp_adapter_add_size(&size, response->reason ? strlen(response->reason) : 0u) != SALTS_OK)
    return SALTS_ERANGE;
  for (index = 0u; index < response->header_count; ++index) {
    if (!response->headers[index].name || !response->headers[index].value ||
        chttp_adapter_add_size(&size, strlen(response->headers[index].name)) != SALTS_OK ||
        chttp_adapter_add_size(&size, strlen(response->headers[index].value)) != SALTS_OK)
      return SALTS_ERANGE;
  }
  if (chttp_adapter_add_size(&size, response->body_size) != SALTS_OK) return SALTS_ERANGE;
  *out_size = size;
  return SALTS_OK;
}

static int chttp_adapter_make_output(const turbo_flow_chttp_slot_t *slot,
                                     const chttp_response_view *response,
                                     turbo_flow_msg_t *output) {
  const turbo_flow_msg_t *input = turbo_flow_async_emit_claim_message(&slot->claim);
  turbo_flow_chttp_response_storage_t *storage;
  turbo_flow_chttp_header_storage_t *header_storage;
  mem_buffer_t *buffer;
  unsigned char *base;
  size_t total_size;
  size_t cursor;
  size_t index;
  int status = chttp_adapter_response_size(response, &total_size);
  if (status != SALTS_OK || !input || !output) return status != SALTS_OK ? status : SALTS_EPROTO;
  buffer = mem_get_buffer(mem_global(), total_size);
  if (!buffer) return SALTS_ENOMEM;
  base = (unsigned char *)mem_buffer_data(buffer);
  memset(base, 0, sizeof(*storage));
  storage = (turbo_flow_chttp_response_storage_t *)base;
  header_storage = (turbo_flow_chttp_header_storage_t *)(base + sizeof(*storage));
  cursor = sizeof(*storage) + response->header_count * sizeof(*header_storage);
  storage->public_context.size = TURBO_FLOW_CHTTP_RESPONSE_CONTEXT_V1_SIZE;
  storage->public_context.version = TURBO_FLOW_CHTTP_CLIENT_API_VERSION;
  storage->public_context.http_major = response->http_major;
  storage->public_context.http_minor = response->http_minor;
  storage->public_context.status_code = response->status_code;
  storage->public_context.header_count = response->header_count;
  storage->public_context.protocol_keep_alive = response->protocol_keep_alive;
  storage->public_context.protocol = slot->owner->protocol;
  storage->public_context.attempts = slot->attempts;
  storage->headers_offset = sizeof(*storage);
  storage->reason_offset = cursor;
  storage->reason_size = response->reason ? strlen(response->reason) : 0u;
  if (storage->reason_size != 0u) {
    memcpy(base + cursor, response->reason, storage->reason_size);
    cursor += storage->reason_size;
  }
  for (index = 0u; index < response->header_count; ++index) {
    header_storage[index].name_offset = cursor;
    header_storage[index].name_size = strlen(response->headers[index].name);
    memcpy(base + cursor, response->headers[index].name, header_storage[index].name_size);
    cursor += header_storage[index].name_size;
    header_storage[index].value_offset = cursor;
    header_storage[index].value_size = strlen(response->headers[index].value);
    memcpy(base + cursor, response->headers[index].value, header_storage[index].value_size);
    cursor += header_storage[index].value_size;
  }
  if (response->body_size != 0u) memcpy(base + cursor, response->body, response->body_size);
  mem_set_used(buffer, total_size);
  turbo_flow_msg_init(output);
  output->id = input->id;
  output->ts_ns = input->ts_ns;
  output->type = input->type;
  output->flags = input->flags;
  output->buffer = buffer;
  output->payload = vstr_from_buf((const char *)base + cursor, response->body_size);
  output->transport_context = &storage->public_context;
  output->status = (int)response->status_code;
  return SALTS_OK;
}

static void chttp_adapter_slot_reset(turbo_flow_chttp_slot_t *slot) {
  turbo_flow_chttp_client_t *owner = slot->owner;
  memset(slot, 0, sizeof(*slot));
  slot->owner = owner;
  slot->claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
}

static void chttp_adapter_take_claim(turbo_flow_chttp_slot_t *slot,
                                     turbo_flow_async_emit_claim_t *out_claim) {
  (void)turbo_flow_async_emit_claim_move(out_claim, &slot->claim);
  chttp_adapter_slot_reset(slot);
}

static void chttp_adapter_finish(turbo_flow_chttp_slot_t *slot, int status,
                                 const chttp_response_view *response, int native_status) {
  turbo_flow_chttp_client_t *client = slot->owner;
  turbo_flow_async_emit_claim_t claim = TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
  turbo_flow_msg_t output;
  int completion_status = status;
  int has_output = 0;

  turbo_flow_msg_init(&output);
  if (status == SALTS_OK && response) {
    completion_status = chttp_adapter_make_output(slot, response, &output);
    has_output = completion_status == SALTS_OK;
  }
  salts_mutex_lock(&client->mutex);
  chttp_adapter_take_claim(slot, &claim);
  chttp_adapter_counter_increment(&client->completed_requests);
  if (has_output) {
    if (response->body_size <= UINT64_MAX - client->response_bytes)
      client->response_bytes += (uint64_t)response->body_size;
    else client->response_bytes = UINT64_MAX;
  }
  client->last_status = completion_status;
  client->last_native_status = native_status;
  salts_mutex_unlock(&client->mutex);
  (void)turbo_flow_async_emit_complete(&claim, completion_status, has_output ? &output : NULL);
  turbo_flow_msg_cleanup(&output);
}

static void chttp_adapter_complete(void *user, chttp_request request,
                                   const chttp_response_view *response, const chttp_error *error) {
  turbo_flow_chttp_slot_t *slot = (turbo_flow_chttp_slot_t *)user;
  turbo_flow_chttp_client_t *client;
  uint64_t now;
  int status;
  int native_status;
  (void)request;
  if (!slot || !(client = slot->owner)) return;
  status = error ? error->status : SALTS_OK;
  native_status = error ? error->native_status : 0;
  now = salts_monotonic_ms();

  salts_mutex_lock(&client->mutex);
  if (!turbo_flow_async_emit_claim_message(&slot->claim)) {
    /* A failed stop already settled the Flow claim; native teardown still
     * owns this callback and slot storage until an explicit stop retry. */
    salts_mutex_unlock(&client->mutex);
    return;
  }
  if (slot->cancel_requested) {
    status = slot->cancel_status;
  } else if (slot->deadline_ms != 0u && now >= slot->deadline_ms) {
    status = SALTS_ETIMEDOUT;
  } else if (status != SALTS_OK &&
             (client->state == TURBO_FLOW_CHTTP_CLIENT_RUNNING ||
              client->state == TURBO_FLOW_CHTTP_CLIENT_QUIESCED) &&
             chttp_adapter_method_idempotent(client) && chttp_adapter_retryable(status) &&
             slot->attempts < client->max_attempts) {
    slot->state = TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT;
    slot->retry_at_ms = chttp_adapter_deadline_after(now, client->retry_delay_ms);
    chttp_adapter_counter_increment(&client->retried_requests);
    client->last_status = status;
    client->last_native_status = native_status;
    salts_mutex_unlock(&client->mutex);
    return;
  }
  slot->state = TURBO_FLOW_CHTTP_SLOT_COMPLETING;
  salts_mutex_unlock(&client->mutex);
  chttp_adapter_finish(slot, status, response, native_status);
}

static int chttp_adapter_start(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  turbo_flow_chttp_client_t *client = (turbo_flow_chttp_client_t *)ctx;
  int status;
  (void)stage;
  if (!client || flow != client->flow) return SALTS_EINVAL;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_REGISTERED) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_EALREADY;
  }
  salts_mutex_unlock(&client->mutex);
  status = chttp_async_client_init(&client->http, &client->config);
  salts_mutex_lock(&client->mutex);
  /* Failed init owns no native callback storage. Preserve the error separately
   * from the stopped resource state used by generation teardown. */
  client->state = status == SALTS_OK  ? TURBO_FLOW_CHTTP_CLIENT_RUNNING
                  : client->http.impl ? TURBO_FLOW_CHTTP_CLIENT_FAILED
                                      : TURBO_FLOW_CHTTP_CLIENT_STOPPED;
  client->last_status = status;
  salts_mutex_unlock(&client->mutex);
  return status;
}

static int chttp_adapter_submit_claim(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage,
                                      const turbo_flow_msg_t *message,
                                      turbo_flow_async_emit_claim_t *claim) {
  turbo_flow_chttp_client_t *client = (turbo_flow_chttp_client_t *)ctx;
  turbo_flow_chttp_slot_t *free_slot = NULL;
  size_t index;
  int status;
  (void)stage;
  if (!client || flow != client->flow || !message || !claim) return SALTS_EINVAL;
  if (message->payload.len != 0u && !message->payload.data) return SALTS_EINVAL;
  if (message->payload.len > client->config.max_request_body_bytes) return SALTS_EMSGSIZE;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_RUNNING) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_ESHUTDOWN;
  }
  for (index = 0u; index < client->slot_count; ++index) {
    const turbo_flow_msg_t *retained;
    if (client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_FREE) {
      if (!free_slot) free_slot = &client->slots[index];
      continue;
    }
    retained = turbo_flow_async_emit_claim_message(&client->slots[index].claim);
    if (retained && retained->id == message->id) {
      salts_mutex_unlock(&client->mutex);
      return SALTS_EALREADY;
    }
  }
  if (!free_slot) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_ENOBUFS;
  }
  status = turbo_flow_async_emit_claim_move(&free_slot->claim, claim);
  if (status == SALTS_OK) {
    free_slot->state = TURBO_FLOW_CHTTP_SLOT_QUEUED;
    free_slot->attempts = 0u;
    free_slot->deadline_ms =
        client->overall_timeout_ms == 0u
            ? 0u
            : chttp_adapter_deadline_after(salts_monotonic_ms(), client->overall_timeout_ms);
  }
  salts_mutex_unlock(&client->mutex);
  return status;
}

static void chttp_adapter_stop(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage) {
  turbo_flow_chttp_client_t *client = (turbo_flow_chttp_client_t *)ctx;
  size_t index;
  int status;
  (void)flow;
  (void)stage;
  if (!client) return;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_RUNNING &&
      client->state != TURBO_FLOW_CHTTP_CLIENT_QUIESCED &&
      client->state != TURBO_FLOW_CHTTP_CLIENT_FAILED) {
    salts_mutex_unlock(&client->mutex);
    return;
  }
  client->state = TURBO_FLOW_CHTTP_CLIENT_STOPPING;
  while (client->owner_active)
    salts_cond_wait(&client->owner_cond, &client->mutex);
  client->owner_active = 1;
  salts_mutex_unlock(&client->mutex);

  for (index = 0u; index < client->slot_count; ++index) {
    turbo_flow_async_emit_claim_t claim = TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
    salts_mutex_lock(&client->mutex);
    if (client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_QUEUED ||
        client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT) {
      chttp_adapter_take_claim(&client->slots[index], &claim);
      chttp_adapter_counter_increment(&client->completed_requests);
      client->last_status = SALTS_ESHUTDOWN;
    }
    salts_mutex_unlock(&client->mutex);
    if (turbo_flow_async_emit_claim_message(&claim))
      (void)turbo_flow_async_emit_complete(&claim, SALTS_ESHUTDOWN, NULL);
  }

  if (client->http.impl) {
    status = chttp_async_client_stop(&client->http, client->stop_timeout_ms);
    if (status == SALTS_OK) status = chttp_async_client_destroy(&client->http);
  } else {
    status = client->last_status;
  }
  if (status != SALTS_OK) {
    /* Flow cannot finish stopping with outstanding emit claims. Native request
     * storage stays alive, but its later callbacks no longer own a Flow claim. */
    for (index = 0u; index < client->slot_count; ++index) {
      salts_mutex_lock(&client->mutex);
      const int pending = turbo_flow_async_emit_claim_message(&client->slots[index].claim) != NULL;
      if (pending) client->slots[index].state = TURBO_FLOW_CHTTP_SLOT_COMPLETING;
      salts_mutex_unlock(&client->mutex);
      if (pending) chttp_adapter_finish(&client->slots[index], status, NULL, 0);
    }
    const int report_status = turbo_flow_adapter_report_stop_status(flow, status);
    if (report_status != SALTS_OK) status = report_status;
  }
  salts_mutex_lock(&client->mutex);
  client->state =
      status == SALTS_OK ? TURBO_FLOW_CHTTP_CLIENT_STOPPED : TURBO_FLOW_CHTTP_CLIENT_FAILED;
  client->last_status = status;
  client->owner_active = 0;
  salts_cond_broadcast(&client->owner_cond);
  salts_mutex_unlock(&client->mutex);
}

static void chttp_adapter_shutdown(void *ctx) {
  turbo_flow_chttp_client_t *client = (turbo_flow_chttp_client_t *)ctx;
  if (!client) return;
  salts_mutex_lock(&client->mutex);
  if (client->state == TURBO_FLOW_CHTTP_CLIENT_REGISTERED ||
      client->state == TURBO_FLOW_CHTTP_CLIENT_STOPPED ||
      (client->state == TURBO_FLOW_CHTTP_CLIENT_FAILED && !client->http.impl))
    client->state = TURBO_FLOW_CHTTP_CLIENT_DETACHED;
  salts_mutex_unlock(&client->mutex);
}

static int chttp_adapter_config_valid(const turbo_flow_chttp_client_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CHTTP_CLIENT_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->client ||
      !config->connection_uri || !config->authority || config->authority[0] == '\0' ||
      !config->target || config->target[0] != '/' || !chttp_adapter_method_valid(config->method) ||
      config->protocol < CHTTP_HTTP_1_1 || config->protocol > CHTTP_HTTP_2 ||
      config->max_attempts == 0u || config->stop_timeout_ms == 0u ||
      config->client->request_capacity == 0u ||
      config->header_count > config->client->max_header_count ||
      (config->header_count != 0u && !config->headers))
    return 0;
  return chttp_adapter_uri_valid(config);
}

int turbo_flow_chttp_client_register(const turbo_flow_chttp_client_config_t *config,
                                     turbo_flow_chttp_client_t **out_client) {
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_emit_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_EMIT_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_chttp_client_t *client;
  size_t index;
  int status;
  if (out_client) *out_client = NULL;
  if (!out_client || !chttp_adapter_config_valid(config)) return SALTS_EINVAL;
  client = (turbo_flow_chttp_client_t *)calloc(1u, sizeof(*client));
  if (!client) return SALTS_ENOMEM;
  salts_mutex_init(&client->mutex);
  salts_cond_init(&client->owner_cond);
  client->flow = config->flow;
  client->config = *config->client;
  client->method = config->method;
  client->tls = config->tls;
  client->protocol = config->protocol;
  client->overall_timeout_ms = config->overall_timeout_ms;
  client->max_attempts = config->max_attempts;
  client->retry_delay_ms = config->retry_delay_ms;
  client->stop_timeout_ms = config->stop_timeout_ms;
  client->idempotent = config->idempotent != 0;
  client->slot_count = config->client->request_capacity;
  client->state = TURBO_FLOW_CHTTP_CLIENT_REGISTERED;
  client->last_status = SALTS_OK;
  client->adapter_name = tstr_dup(config->adapter_name);
  client->connection_uri = tstr_dup(config->connection_uri);
  client->authority = tstr_dup(config->authority);
  client->target = tstr_dup(config->target);
  client->slots = (turbo_flow_chttp_slot_t *)calloc(client->slot_count, sizeof(*client->slots));
  if (!client->adapter_name || !client->connection_uri || !client->authority || !client->target ||
      !client->slots) {
    status = SALTS_ENOMEM;
    goto fail;
  }
  for (index = 0u; index < client->slot_count; ++index) {
    client->slots[index].owner = client;
    client->slots[index].claim = (turbo_flow_async_emit_claim_t)TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
  }
  status = chttp_adapter_copy_headers(client, config->headers, config->header_count);
  if (status != SALTS_OK) goto fail;

  adapter_ops.start = chttp_adapter_start;
  adapter_ops.stop = chttp_adapter_stop;
  adapter_ops.shutdown = chttp_adapter_shutdown;
  async_ops.submit = chttp_adapter_submit_claim;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_HTTP;
  schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  status = turbo_flow_register_async_emit_adapter_ex(config->flow, config->adapter_name,
                                                     &adapter_ops, &async_ops, client, &schema);
  if (status != SALTS_OK) goto fail;
  *out_client = client;
  return SALTS_OK;

fail:
  chttp_adapter_client_cleanup(client);
  salts_cond_destroy(&client->owner_cond);
  salts_mutex_destroy(&client->mutex);
  free(client);
  return status;
}

static int chttp_adapter_poll_slot(turbo_flow_chttp_slot_t *slot, uint64_t now) {
  turbo_flow_chttp_client_t *client = slot->owner;
  const turbo_flow_msg_t *message;
  chttp_request_options options = {0};
  int status;

  salts_mutex_lock(&client->mutex);
  if (slot->state == TURBO_FLOW_CHTTP_SLOT_FREE || slot->state == TURBO_FLOW_CHTTP_SLOT_ACTIVE) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_OK;
  }
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_RUNNING &&
      client->state != TURBO_FLOW_CHTTP_CLIENT_QUIESCED) {
    slot->state = TURBO_FLOW_CHTTP_SLOT_COMPLETING;
    salts_mutex_unlock(&client->mutex);
    chttp_adapter_finish(slot, SALTS_ESHUTDOWN, NULL, 0);
    return SALTS_OK;
  }
  if (slot->cancel_requested || (slot->deadline_ms != 0u && now >= slot->deadline_ms)) {
    const int terminal_status = slot->cancel_requested ? slot->cancel_status : SALTS_ETIMEDOUT;
    slot->state = TURBO_FLOW_CHTTP_SLOT_COMPLETING;
    salts_mutex_unlock(&client->mutex);
    chttp_adapter_finish(slot, terminal_status, NULL, 0);
    return SALTS_OK;
  }
  if (slot->state == TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT && now < slot->retry_at_ms) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_OK;
  }
  message = turbo_flow_async_emit_claim_message(&slot->claim);
  if (!message) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_EPROTO;
  }
  options.connection_uri = client->connection_uri;
  options.authority = client->authority;
  options.target = client->target;
  options.method = client->method;
  options.headers = client->headers;
  options.header_count = client->header_count;
  options.body = message->payload.data;
  options.body_size = message->payload.len;
  options.on_complete = chttp_adapter_complete;
  options.user = slot;
  options.tls = client->tls;
  options.protocol = client->protocol;
  ++slot->attempts;
  salts_mutex_unlock(&client->mutex);

  status = chttp_async_client_submit(&client->http, &options, &slot->request);
  salts_mutex_lock(&client->mutex);
  if (status == SALTS_OK) {
    slot->state = TURBO_FLOW_CHTTP_SLOT_ACTIVE;
    chttp_adapter_counter_increment(&client->submitted_requests);
    salts_mutex_unlock(&client->mutex);
    return SALTS_OK;
  }
  if (status == SALTS_ENOBUFS) {
    --slot->attempts;
    slot->state = TURBO_FLOW_CHTTP_SLOT_QUEUED;
    salts_mutex_unlock(&client->mutex);
    return SALTS_OK;
  }
  if (chttp_adapter_method_idempotent(client) && chttp_adapter_retryable(status) &&
      slot->attempts < client->max_attempts) {
    slot->state = TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT;
    slot->retry_at_ms = chttp_adapter_deadline_after(now, client->retry_delay_ms);
    chttp_adapter_counter_increment(&client->retried_requests);
    client->last_status = status;
    salts_mutex_unlock(&client->mutex);
    return SALTS_OK;
  }
  slot->state = TURBO_FLOW_CHTTP_SLOT_COMPLETING;
  salts_mutex_unlock(&client->mutex);
  chttp_adapter_finish(slot, status, NULL, 0);
  return SALTS_OK;
}

static int chttp_adapter_poll_cancellations(turbo_flow_chttp_client_t *client, uint64_t now) {
  size_t index;
  int first_status = SALTS_OK;
  for (index = 0u; index < client->slot_count; ++index) {
    turbo_flow_chttp_slot_t *slot = &client->slots[index];
    int should_cancel = 0;
    salts_mutex_lock(&client->mutex);
    if (slot->state == TURBO_FLOW_CHTTP_SLOT_ACTIVE) {
      if (slot->cancel_requested) {
        should_cancel = 1;
      } else if (slot->deadline_ms != 0u && now >= slot->deadline_ms) {
        slot->cancel_requested = 1;
        slot->cancel_status = SALTS_ETIMEDOUT;
        should_cancel = 1;
      }
    }
    salts_mutex_unlock(&client->mutex);
    if (should_cancel) {
      const int status = chttp_async_request_cancel(&client->http, slot->request);
      if (status != SALTS_OK && status != SALTS_EALREADY && status != SALTS_ENOENT &&
          first_status == SALTS_OK)
        first_status = status;
    }
  }
  return first_status;
}

static uint32_t chttp_adapter_poll_timeout(turbo_flow_chttp_client_t *client, uint64_t now,
                                           uint32_t requested_timeout_ms) {
  uint32_t bounded_timeout_ms = requested_timeout_ms;
  size_t index;
  salts_mutex_lock(&client->mutex);
  for (index = 0u; index < client->slot_count; ++index) {
    const turbo_flow_chttp_slot_t *slot = &client->slots[index];
    uint64_t remaining_ms;
    if (slot->state == TURBO_FLOW_CHTTP_SLOT_FREE ||
        slot->state == TURBO_FLOW_CHTTP_SLOT_COMPLETING || slot->deadline_ms == 0u)
      continue;
    remaining_ms = slot->deadline_ms > now ? slot->deadline_ms - now : 0u;
    if (remaining_ms < bounded_timeout_ms) bounded_timeout_ms = (uint32_t)remaining_ms;
  }
  salts_mutex_unlock(&client->mutex);
  return bounded_timeout_ms;
}

int turbo_flow_chttp_client_poll(turbo_flow_chttp_client_t *client, uint32_t timeout_ms,
                                 turbo_flow_chttp_client_snapshot_t *out_snapshot) {
  size_t completions = 0u;
  size_t index;
  uint64_t now;
  int status;
  if (!client) return SALTS_EINVAL;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_RUNNING &&
      client->state != TURBO_FLOW_CHTTP_CLIENT_QUIESCED) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_ESHUTDOWN;
  }
  if (client->owner_active) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_EBUSY;
  }
  client->owner_active = 1;
  salts_mutex_unlock(&client->mutex);
  now = salts_monotonic_ms();
  status = chttp_adapter_poll_cancellations(client, now);
  if (status != SALTS_OK) goto done;
  for (index = 0u; index < client->slot_count; ++index) {
    status = chttp_adapter_poll_slot(&client->slots[index], now);
    if (status != SALTS_OK) goto done;
  }
  timeout_ms = chttp_adapter_poll_timeout(client, now, timeout_ms);
  status = chttp_async_client_poll(&client->http, timeout_ms, &completions);
  if (status != SALTS_OK) goto done;
  now = salts_monotonic_ms();
  status = chttp_adapter_poll_cancellations(client, now);
  if (status != SALTS_OK) goto done;
  for (index = 0u; index < client->slot_count; ++index) {
    status = chttp_adapter_poll_slot(&client->slots[index], now);
    if (status != SALTS_OK) goto done;
  }
done:
  salts_mutex_lock(&client->mutex);
  client->owner_active = 0;
  salts_cond_broadcast(&client->owner_cond);
  salts_mutex_unlock(&client->mutex);
  if (status == SALTS_OK && out_snapshot)
    status = turbo_flow_chttp_client_snapshot(client, out_snapshot);
  return status;
}

static int chttp_adapter_admission(turbo_flow_chttp_client_t *client, int enabled) {
  if (!client) return SALTS_EINVAL;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_RUNNING &&
      client->state != TURBO_FLOW_CHTTP_CLIENT_QUIESCED) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_ESHUTDOWN;
  }
  client->state = enabled ? TURBO_FLOW_CHTTP_CLIENT_RUNNING : TURBO_FLOW_CHTTP_CLIENT_QUIESCED;
  salts_mutex_unlock(&client->mutex);
  return SALTS_OK;
}
int turbo_flow_chttp_client_quiesce(turbo_flow_chttp_client_t *client) {
  return chttp_adapter_admission(client, 0);
}
int turbo_flow_chttp_client_resume(turbo_flow_chttp_client_t *client) {
  return chttp_adapter_admission(client, 1);
}
int turbo_flow_chttp_client_cancel(turbo_flow_chttp_client_t *client, uint64_t message_id) {
  size_t index;
  int status = SALTS_ENOENT;
  if (!client) return SALTS_EINVAL;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_RUNNING &&
      client->state != TURBO_FLOW_CHTTP_CLIENT_QUIESCED) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_ESHUTDOWN;
  }
  for (index = 0u; index < client->slot_count; ++index) {
    const turbo_flow_msg_t *message =
        turbo_flow_async_emit_claim_message(&client->slots[index].claim);
    if (!message || message->id != message_id) continue;
    if (client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_COMPLETING) {
      status = SALTS_ENOENT;
    } else if (client->slots[index].cancel_requested) {
      status = SALTS_EALREADY;
    } else {
      client->slots[index].cancel_requested = 1;
      client->slots[index].cancel_status = SALTS_ECANCELED;
      chttp_adapter_counter_increment(&client->canceled_requests);
      status = SALTS_OK;
    }
    break;
  }
  salts_mutex_unlock(&client->mutex);
  return status;
}

int turbo_flow_chttp_client_snapshot(const turbo_flow_chttp_client_t *client,
                                     turbo_flow_chttp_client_snapshot_t *out_snapshot) {
  turbo_flow_chttp_client_t *mutable_client = (turbo_flow_chttp_client_t *)client;
  size_t index;
  if (!client || !out_snapshot || out_snapshot->size < sizeof(*out_snapshot) ||
      out_snapshot->version != TURBO_FLOW_CHTTP_CLIENT_API_VERSION)
    return SALTS_EINVAL;
  salts_mutex_lock(&mutable_client->mutex);
  out_snapshot->state = client->state;
  out_snapshot->active_requests = 0u;
  out_snapshot->queued_requests = 0u;
  for (index = 0u; index < client->slot_count; ++index) {
    if (client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_ACTIVE) ++out_snapshot->active_requests;
    else if (client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_QUEUED ||
             client->slots[index].state == TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT)
      ++out_snapshot->queued_requests;
  }
  out_snapshot->submitted_requests = client->submitted_requests;
  out_snapshot->completed_requests = client->completed_requests;
  out_snapshot->retried_requests = client->retried_requests;
  out_snapshot->canceled_requests = client->canceled_requests;
  out_snapshot->response_bytes = client->response_bytes;
  out_snapshot->last_status = client->last_status;
  out_snapshot->last_native_status = client->last_native_status;
  salts_mutex_unlock(&mutable_client->mutex);
  return SALTS_OK;
}

int turbo_flow_chttp_client_destroy(turbo_flow_chttp_client_t *client) {
  if (!client) return SALTS_EINVAL;
  salts_mutex_lock(&client->mutex);
  if (client->state != TURBO_FLOW_CHTTP_CLIENT_DETACHED) {
    salts_mutex_unlock(&client->mutex);
    return SALTS_EBUSY;
  }
  salts_mutex_unlock(&client->mutex);
  chttp_adapter_client_cleanup(client);
  salts_cond_destroy(&client->owner_cond);
  salts_mutex_destroy(&client->mutex);
  free(client);
  return SALTS_OK;
}

static const turbo_flow_chttp_response_storage_t *
chttp_adapter_response_storage(const turbo_flow_msg_t *message) {
  const turbo_flow_chttp_response_storage_t *storage;
  if (!message || !message->buffer || !message->transport_context ||
      message->transport_context != mem_buffer_const_data(message->buffer) ||
      mem_buffer_used(message->buffer) < sizeof(*storage))
    return NULL;
  storage = (const turbo_flow_chttp_response_storage_t *)message->transport_context;
  if (storage->public_context.size < sizeof(storage->public_context) ||
      storage->public_context.version != TURBO_FLOW_CHTTP_CLIENT_API_VERSION ||
      storage->headers_offset != sizeof(*storage) ||
      storage->public_context.header_count > (mem_buffer_used(message->buffer) - sizeof(*storage)) /
                                                 sizeof(turbo_flow_chttp_header_storage_t))
    return NULL;
  return storage;
}

const turbo_flow_chttp_response_context_t *
turbo_flow_chttp_response_context(const turbo_flow_msg_t *message) {
  const turbo_flow_chttp_response_storage_t *storage = chttp_adapter_response_storage(message);
  return storage ? &storage->public_context : NULL;
}

int turbo_flow_chttp_response_reason(const turbo_flow_msg_t *message, vstr *out_reason) {
  const turbo_flow_chttp_response_storage_t *storage = chttp_adapter_response_storage(message);
  const char *base;
  size_t used;
  if (!storage || !out_reason) return SALTS_EINVAL;
  base = mem_buffer_const_data(message->buffer);
  used = mem_buffer_used(message->buffer);
  if (storage->reason_offset > used || storage->reason_size > used - storage->reason_offset)
    return SALTS_EPROTO;
  *out_reason = vstr_from_buf(base + storage->reason_offset, storage->reason_size);
  return SALTS_OK;
}

int turbo_flow_chttp_response_header_at(const turbo_flow_msg_t *message, size_t index,
                                        vstr *out_name, vstr *out_value) {
  const turbo_flow_chttp_response_storage_t *storage = chttp_adapter_response_storage(message);
  const turbo_flow_chttp_header_storage_t *headers;
  const char *base;
  size_t used;
  if (!storage || !out_name || !out_value || index >= storage->public_context.header_count)
    return SALTS_EINVAL;
  base = mem_buffer_const_data(message->buffer);
  used = mem_buffer_used(message->buffer);
  headers = (const turbo_flow_chttp_header_storage_t *)(base + storage->headers_offset);
  if (headers[index].name_offset > used ||
      headers[index].name_size > used - headers[index].name_offset ||
      headers[index].value_offset > used ||
      headers[index].value_size > used - headers[index].value_offset)
    return SALTS_EPROTO;
  *out_name = vstr_from_buf(base + headers[index].name_offset, headers[index].name_size);
  *out_value = vstr_from_buf(base + headers[index].value_offset, headers[index].value_size);
  return SALTS_OK;
}
