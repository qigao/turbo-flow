#ifndef TURBO_FLOW_CHTTP_CLIENT_INTERNAL_H
#define TURBO_FLOW_CHTTP_CLIENT_INTERNAL_H

#include "turbo_flow_chttp.h"
#include <salts/thread.h>

typedef enum turbo_flow_chttp_slot_state_e {
  TURBO_FLOW_CHTTP_SLOT_FREE = 0,
  TURBO_FLOW_CHTTP_SLOT_QUEUED,
  TURBO_FLOW_CHTTP_SLOT_ACTIVE,
  TURBO_FLOW_CHTTP_SLOT_RETRY_WAIT,
  TURBO_FLOW_CHTTP_SLOT_COMPLETING,
  /* Flow claim is settled, but native callback still borrows this slot. */
  TURBO_FLOW_CHTTP_SLOT_DRAINING
} turbo_flow_chttp_slot_state_t;

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

/* Private fault admission, configured only by formal lifecycle tests while
 * no progress/lifecycle owner is active. Success executes the real SDK call. */
typedef struct turbo_flow_chttp_shutdown_fault_s {
  int (*before_stop)(void *ctx);
  int (*before_destroy)(void *ctx);
  void *ctx;
} turbo_flow_chttp_shutdown_fault_t;

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
  cmeta_mutex_t mutex;
  cmeta_cond_t owner_cond;
  int owner_active;
  turbo_flow_chttp_client_state_t state;
  uint64_t submitted_requests;
  uint64_t completed_requests;
  uint64_t retried_requests;
  uint64_t canceled_requests;
  uint64_t response_bytes;
  int last_status;
  int last_native_status;
  turbo_flow_chttp_shutdown_fault_t shutdown_fault;
};

#endif
