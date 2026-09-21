#ifndef FLOW_INBOX_DRIVER_INTERNAL_H
#define FLOW_INBOX_DRIVER_INTERNAL_H

#include "turbo_flow_inbox_source.h"

typedef enum flow_inbox_driver_origin_kind_e {
  FLOW_INBOX_DRIVER_SOURCE = 1,
  FLOW_INBOX_DRIVER_BUFFER = 2
} flow_inbox_driver_origin_kind_t;

typedef int (*flow_inbox_driver_claim_begin_fn)(void *ctx);
typedef void (*flow_inbox_driver_claim_end_fn)(void *ctx, int observed,
                                                int claim_status, uint64_t record_id);
typedef void (*flow_inbox_driver_completion_fn)(void *ctx, int status);

typedef struct flow_inbox_driver_config_s {
  turbo_flow_inbox_t *inbox;
  turbo_flow_t *flow;
  flow_inbox_driver_origin_kind_t origin_kind;
  uint32_t origin_stage;
  cflow_scheduler *scheduler;
  size_t max_message_bytes;
  flow_inbox_driver_claim_begin_fn claim_begin;
  flow_inbox_driver_claim_end_fn claim_end;
  void *claim_observer_ctx;
  flow_inbox_driver_completion_fn graph_complete;
  flow_inbox_driver_completion_fn sink_complete;
  void *completion_observer_ctx;
} flow_inbox_driver_config_t;

typedef struct flow_inbox_driver_s flow_inbox_driver_t;

int flow_inbox_driver_create(const flow_inbox_driver_config_t *config,
                             flow_inbox_driver_t **driver_out);
int flow_inbox_driver_request(flow_inbox_driver_t *driver);
/* Buffer-only eligible claim using the exact provider selector. */
int flow_inbox_driver_request_ex(flow_inbox_driver_t *driver,
                                 const turbo_flow_inbox_claim_request_t *request);
/* Only a buffer drain may use paused Graph admission. */
int flow_inbox_driver_request_drain(flow_inbox_driver_t *driver);
int flow_inbox_driver_request_drain_ex(flow_inbox_driver_t *driver,
                                       const turbo_flow_inbox_claim_request_t *request);
/* Borrow the active claim partition while the driver is non-idle. */
int flow_inbox_driver_active_partition(const flow_inbox_driver_t *driver, vstr *partition_out);
/* Read-only phase/status; never settles, retries, or claims storage. */
int flow_inbox_driver_status(const flow_inbox_driver_t *driver,
                             turbo_flow_inbox_source_result_t *result);
int flow_inbox_driver_poll(flow_inbox_driver_t *driver,
                           turbo_flow_inbox_source_result_t *result);
int flow_inbox_driver_cancel(flow_inbox_driver_t *driver,
                             turbo_flow_inbox_source_result_t *result);
int flow_inbox_driver_retry_settlement(flow_inbox_driver_t *driver,
                                       turbo_flow_inbox_source_result_t *result);
int flow_inbox_driver_reconcile_settlement(flow_inbox_driver_t *driver,
                                           turbo_flow_inbox_source_result_t *result);
int flow_inbox_driver_destroy(flow_inbox_driver_t *driver);

#endif
