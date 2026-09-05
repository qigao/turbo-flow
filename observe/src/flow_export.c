#include "turbo_flow_observe.h"

#include "salts_error.h"

#include <stdio.h>
#include <string.h>

static int flow_export_write(turbo_flow_observe_write_fn write, void *ctx,
                             const char *data, size_t len) {
  return write(ctx, data, len);
}

static int flow_export_prometheus_escape(const char *input, char *out, size_t out_size) {
  size_t used = 0u;
  if (!input || !out || out_size == 0u) return SALTS_EINVAL;
  for (size_t i = 0; input[i] != '\0'; ++i) {
    const char *replacement = NULL;
    size_t replacement_len = 0u;
    if (input[i] == '\\') {
      replacement = "\\\\";
      replacement_len = 2u;
    } else if (input[i] == '"') {
      replacement = "\\\"";
      replacement_len = 2u;
    } else if (input[i] == '\n') {
      replacement = "\\n";
      replacement_len = 2u;
    }
    if (replacement) {
      if (replacement_len >= out_size - used) return SALTS_ENAMETOOLONG;
      memcpy(out + used, replacement, replacement_len);
      used += replacement_len;
    } else {
      if (used + 1u >= out_size) return SALTS_ENAMETOOLONG;
      out[used++] = input[i];
    }
  }
  out[used] = '\0';
  return SALTS_OK;
}

static int flow_export_prometheus_resource(turbo_flow_observe_write_fn write, void *ctx,
                                           const turbo_flow_resource_snapshot_t *resource) {
  char uid[(TURBO_FLOW_RESOURCE_UID_MAX + 1u) * 2u];
  char owner[(TURBO_FLOW_RESOURCE_OWNER_MAX + 1u) * 2u];
  char line[1536];
  static const char *const names[] = {
      "turbo_flow_resource_load", "turbo_flow_resource_capacity",
      "turbo_flow_resource_saturated", "turbo_flow_resource_generation",
      "turbo_flow_resource_observed_generation", "turbo_flow_resource_last_status"};
  uint64_t values[] = {resource->load, resource->capacity, (uint64_t)(resource->saturated != 0),
                       resource->generation, resource->observed_generation,
                       (uint64_t)(int64_t)resource->last_status};
  int rc = flow_export_prometheus_escape(resource->uid, uid, sizeof(uid));
  if (rc != SALTS_OK) return rc;
  rc = flow_export_prometheus_escape(resource->owner_name, owner, sizeof(owner));
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
    int written;
    if (i == 5u) {
      written = snprintf(line, sizeof(line),
                         "%s{uid=\"%s\",owner=\"%s\",kind=\"%u\"} %d\n", names[i], uid,
                         owner, (unsigned)resource->kind, resource->last_status);
    } else {
      written = snprintf(line, sizeof(line),
                         "%s{uid=\"%s\",owner=\"%s\",kind=\"%u\"} %llu\n", names[i], uid,
                         owner, (unsigned)resource->kind, (unsigned long long)values[i]);
    }
    if (written < 0 || (size_t)written >= sizeof(line)) return SALTS_ENAMETOOLONG;
    rc = flow_export_write(write, ctx, line, (size_t)written);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

int turbo_flow_observe_export_prometheus(const turbo_flow_observe_t *observe,
                                         turbo_flow_observe_write_fn write, void *ctx) {
  turbo_flow_observe_snapshot_t traffic;
  char line[128];
  int written;
  int rc;
  if (!observe || !write) return SALTS_EINVAL;
  rc = turbo_flow_observe_snapshot(observe, &traffic);
  if (rc != SALTS_OK) return rc;
  written = snprintf(line, sizeof(line), "turbo_flow_messages_total %llu\n",
                     (unsigned long long)traffic.messages);
  if (written < 0 || (size_t)written >= sizeof(line)) return SALTS_ERANGE;
  rc = flow_export_write(write, ctx, line, (size_t)written);
  if (rc != SALTS_OK) return rc;
  written = snprintf(line, sizeof(line), "turbo_flow_message_errors_total %llu\n",
                     (unsigned long long)traffic.message_errors);
  if (written < 0 || (size_t)written >= sizeof(line)) return SALTS_ERANGE;
  rc = flow_export_write(write, ctx, line, (size_t)written);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < turbo_flow_observe_resource_count(observe); ++i) {
    turbo_flow_observe_resource_view_t resource;
    rc = turbo_flow_observe_resource_at(observe, i, &resource);
    if (rc != SALTS_OK) return rc;
    rc = flow_export_prometheus_resource(write, ctx, &resource.snapshot);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

static int flow_export_emit(turbo_flow_observe_metric_fn emit, void *ctx, const char *name,
                            double value, const turbo_flow_resource_snapshot_t *resource) {
  turbo_flow_observe_metric_t metric;
  memset(&metric, 0, sizeof(metric));
  metric.name = name;
  metric.value = value;
  if (resource) {
    metric.resource_uid = resource->uid;
    metric.owner_name = resource->owner_name;
    metric.resource_kind = resource->kind;
  }
  return emit(ctx, &metric);
}

int turbo_flow_observe_export_opentelemetry(const turbo_flow_observe_t *observe,
                                            turbo_flow_observe_metric_fn emit, void *ctx) {
  turbo_flow_observe_snapshot_t traffic;
  int rc;
  if (!observe || !emit) return SALTS_EINVAL;
  rc = turbo_flow_observe_snapshot(observe, &traffic);
  if (rc != SALTS_OK) return rc;
  rc = flow_export_emit(emit, ctx, "turbo.flow.messages", (double)traffic.messages, NULL);
  if (rc != SALTS_OK) return rc;
  rc = flow_export_emit(emit, ctx, "turbo.flow.message.errors",
                        (double)traffic.message_errors, NULL);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0; i < turbo_flow_observe_resource_count(observe); ++i) {
    turbo_flow_observe_resource_view_t view;
    rc = turbo_flow_observe_resource_at(observe, i, &view);
    if (rc != SALTS_OK) return rc;
    rc = flow_export_emit(emit, ctx, "turbo.flow.resource.load", (double)view.snapshot.load,
                          &view.snapshot);
    if (rc != SALTS_OK) return rc;
    rc = flow_export_emit(emit, ctx, "turbo.flow.resource.capacity",
                          (double)view.snapshot.capacity, &view.snapshot);
    if (rc != SALTS_OK) return rc;
    rc = flow_export_emit(emit, ctx, "turbo.flow.resource.saturated",
                          view.snapshot.saturated ? 1.0 : 0.0, &view.snapshot);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}
