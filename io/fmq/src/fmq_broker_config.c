#include "turbo_flow_fmq_broker.h"

#include "fmq_pattern_config.h"
#include "turbo_error.h"

int turbo_flow_fmq_broker_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                          const char *channel_name, turbo_flow_fmq_broker_t **out,
                                          turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern == FLOW_FMQ_PATTERN_PUBSUB_STATE ||
      config.pattern == FLOW_FMQ_PATTERN_CREDIT_WORKER) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                         "broker requires a request pattern");
  }
  *out = turbo_flow_fmq_broker_create(&config.broker);
  return *out ? TURBO_OK
              : flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                              "FMQ broker creation failed");
}

int turbo_flow_fmq_credit_worker_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                                 const char *channel_name,
                                                 turbo_flow_fmq_credit_worker_t **out,
                                                 turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern != FLOW_FMQ_PATTERN_CREDIT_WORKER) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "pattern",
                                         "credit owner requires pattern credit_worker");
  }
  if (config.credit.reliability == TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE) {
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "use durable_create_resolved with a storage binding");
  }
  *out = turbo_flow_fmq_credit_worker_create(&config.credit);
  return *out ? TURBO_OK
              : flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                              "FMQ credit worker creation failed");
}

int turbo_flow_fmq_credit_durable_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_fmq_credit_durable_binding_t *binding,
    turbo_flow_fmq_credit_worker_t **credit_out, turbo_flow_fmq_credit_durable_t **durable_out,
    turbo_flow_config_error_t *error) {
  flow_fmq_pattern_config_t config;
  turbo_flow_fmq_credit_worker_t *credit;
  int rc;
  if (credit_out) *credit_out = NULL;
  if (durable_out) *durable_out = NULL;
  if (!credit_out || !durable_out || !binding || binding->size < sizeof(*binding) ||
      !binding->storage_channel || !binding->settler)
    return TURBO_EINVAL;
  rc = flow_fmq_pattern_config_resolve(resolved, channel_name, &config, error);
  if (rc != TURBO_OK) return rc;
  if (config.pattern != FLOW_FMQ_PATTERN_CREDIT_WORKER ||
      config.credit.reliability != TURBO_FLOW_FMQ_BROKER_RELIABILITY_AT_LEAST_ONCE)
    return flow_fmq_pattern_config_error(error, TURBO_ENOTSUP, channel_name, "reliability",
                                         "durable owner requires at_least_once credit_worker");
  if (strcmp(config.durable_storage_channel, binding->storage_channel) != 0)
    return flow_fmq_pattern_config_error(error, TURBO_EINVAL, channel_name, "storage_channel",
                                         "storage binding does not match configured channel");
  credit = turbo_flow_fmq_credit_worker_create(&config.credit);
  if (!credit)
    return flow_fmq_pattern_config_error(error, TURBO_ENOMEM, channel_name, NULL,
                                         "FMQ credit worker creation failed");
  rc = turbo_flow_fmq_credit_durable_create(credit, binding->settler, &config.durable, durable_out);
  if (rc != TURBO_OK) {
    turbo_flow_fmq_credit_worker_destroy(credit);
    return flow_fmq_pattern_config_error(error, rc, channel_name, "storage_channel",
                                         "durable storage state could not be opened");
  }
  *credit_out = credit;
  return TURBO_OK;
}
