#include "turbo_flow_cnet_plugin_internal.h"

#include <cstl/vec.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

typedef struct cnet_plugin_root_s cnet_plugin_root_t;
typedef struct cnet_plugin_owner_s cnet_plugin_owner_t;

typedef struct cnet_plugin_provider_s {
  cnet_plugin_root_t *root;
  turbo_flow_cnet_plugin_kind_t kind;
} cnet_plugin_provider_t;

struct cnet_plugin_root_s {
  const turbo_flow_plugin_host_v1_t *host;
  vec_t owners;
  cnet_plugin_provider_t providers[TURBO_FLOW_CNET_PLUGIN_KIND_COUNT];
  int quiesced;
};

struct cnet_plugin_owner_s {
  cnet_plugin_root_t *root;
  turbo_flow_cnet_plugin_config_t config;
  char name[TURBO_FLOW_CNET_PLUGIN_NAME_CAPACITY];
  char source_name[TURBO_FLOW_CNET_PLUGIN_NAME_CAPACITY];
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_state_t state;
  int last_status;
  int quiesced;
  int initial_demand_pending;
  union {
    turbo_flow_cnet_stream_source_t *stream_source;
    turbo_flow_cnet_listener_source_t *listener_source;
    turbo_flow_cnet_packet_source_t *packet_source;
    turbo_flow_cnet_stream_sink_t *stream_sink;
    turbo_flow_cnet_datagram_sink_t *datagram_sink;
    turbo_flow_cnet_packet_sink_t *packet_sink;
    void *any;
  } handle;
};

static int cnet_plugin_stl_status(stl_status status) {
  if (status == STL_OK) return SALTS_OK;
  if (status == STL_OUT_OF_MEMORY) return SALTS_ENOMEM;
  if (status == STL_CAPACITY_EXCEEDED) return SALTS_ENOSPC;
  return SALTS_EINVAL;
}

static int cnet_plugin_error(turbo_flow_config_error_t *error, int status, const char *name,
                             const char *phase, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.%s", name ? name : "unknown",
                   phase ? phase : "provider");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int cnet_plugin_root_add_owner(cnet_plugin_root_t *root, cnet_plugin_owner_t *owner) {
  if (!root || !owner || root->quiesced) return SALTS_ESHUTDOWN;
  if (vec_size(&root->owners) >= TURBO_FLOW_CNET_PLUGIN_MAX_OWNERS) return SALTS_ENOSPC;
  return cnet_plugin_stl_status(vec_push(&root->owners, &owner));
}

static void cnet_plugin_root_remove_owner(cnet_plugin_root_t *root,
                                          const cnet_plugin_owner_t *owner) {
  if (!root || !owner) return;
  for (size_t i = 0u; i < vec_size(&root->owners); ++i) {
    cnet_plugin_owner_t *const *entry =
        (cnet_plugin_owner_t *const *)vec_at_const(&root->owners, i);
    if (entry && *entry == owner) {
      (void)vec_erase(&root->owners, i, NULL);
      return;
    }
  }
}

static int cnet_plugin_reference_validate(const turbo_flow_t *flow, const char *name,
                                          turbo_flow_cnet_plugin_kind_t kind,
                                          turbo_flow_config_error_t *error) {
  const int expected_source = kind <= TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE;
  size_t count = 0u;
  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (!stage || !stage->adapter_name || strcmp(stage->adapter_name, name) != 0) continue;
    if ((stage->is_source != 0) != expected_source)
      return cnet_plugin_error(error, SALTS_EINVAL, name, "role",
                               "CNet provider kind does not match the Graph stage role");
    ++count;
  }
  if (count != 1u)
    return cnet_plugin_error(error, SALTS_EINVAL, name, "references",
                             "each CNet adapter instance must bind exactly one Graph stage");
  return SALTS_OK;
}

static int cnet_plugin_boundary_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = owner->metadata;
  return SALTS_OK;
}

static int cnet_plugin_boundary_descriptor(void *ctx,
                                           turbo_flow_managed_boundary_descriptor_t *out) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = owner->descriptor;
  return SALTS_OK;
}

static int cnet_plugin_boundary_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  int rc = SALTS_OK;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  (void)snprintf(snapshot.uid, sizeof(snapshot.uid), "%s", owner->metadata.uid);
  snapshot.generation = owner->metadata.generation;
  snapshot.observed_generation = owner->metadata.observed_generation;
  snapshot.state = owner->state;
  snapshot.last_status = owner->last_status;
  switch (owner->config.kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE:
    if (owner->handle.stream_source) {
      turbo_flow_cnet_stream_source_snapshot_t source = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
      rc = turbo_flow_cnet_stream_source_snapshot(owner->handle.stream_source, &source);
      snapshot.demand = source.outstanding_demand;
      snapshot.in_flight = source.receive_pending != 0;
      snapshot.accepted = source.messages_received;
      snapshot.last_status = source.status;
    }
    break;
  case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE:
    if (owner->handle.listener_source) {
      turbo_flow_cnet_listener_source_snapshot_t source =
          TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
      rc = turbo_flow_cnet_listener_source_snapshot(owner->handle.listener_source, &source);
      snapshot.demand = source.outstanding_demand;
      snapshot.in_flight = source.receive_pending != 0;
      snapshot.accepted = source.messages_received;
      snapshot.last_status = source.status;
    }
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE:
    snapshot.queue_capacity = owner->config.queue_capacity;
    if (owner->handle.packet_source) {
      turbo_flow_cnet_packet_source_snapshot_t source = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
      rc = turbo_flow_cnet_packet_source_snapshot(owner->handle.packet_source, &source);
      snapshot.demand = source.outstanding_demand;
      snapshot.queue_depth = source.queue_depth;
      snapshot.queue_capacity = source.queue_capacity;
      snapshot.accepted = source.messages_received;
      snapshot.backpressured = source.queue_depth >= source.queue_capacity;
      snapshot.last_status = source.status;
    }
    break;
  default:
    return SALTS_EINVAL;
  }
  if (rc != SALTS_OK) return rc;
  *out = snapshot;
  return SALTS_OK;
}

static int cnet_plugin_source_start(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  int rc = SALTS_EINVAL;
  if (!owner || !flow || !stage || owner->handle.any || owner->quiesced) return SALTS_EINVAL;
  if (!stage->name || strlen(stage->name) >= sizeof(owner->source_name)) return SALTS_ERANGE;
  memcpy(owner->source_name, stage->name, strlen(stage->name) + 1u);
  owner->state = TURBO_FLOW_MANAGED_BOUNDARY_STARTING;
  switch (owner->config.kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE: {
    turbo_flow_cnet_stream_source_config_t source = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
    source.flow = flow;
    source.source_name = owner->source_name;
    source.uri = owner->config.uri;
    source.client = &owner->config.client;
    source.socket_options = &owner->config.socket_options;
    source.tls = owner->config.tls_enabled ? &owner->config.tls_client : NULL;
    source.content = &owner->config.content;
    source.max_message_bytes = owner->config.max_message_bytes;
    source.scheduler_capacity = owner->config.scheduler_capacity;
    source.scheduler_max_steps_per_poll = owner->config.scheduler_max_steps_per_poll;
    source.first_message_id = owner->config.first_message_id;
    rc = turbo_flow_cnet_stream_source_open_managed(&source, stage, &owner->handle.stream_source);
    break;
  }
  case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE: {
    turbo_flow_cnet_listener_source_config_t source = TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
    source.flow = flow;
    source.source_name = owner->source_name;
    source.listener = &owner->config.listener;
    source.listener_options = &owner->config.listener_options;
    source.client = &owner->config.client;
    source.socket_options = &owner->config.socket_options;
    source.tls = owner->config.tls_enabled ? &owner->config.tls_server : NULL;
    source.content = &owner->config.content;
    source.max_connections = owner->config.max_connections;
    source.max_message_bytes = owner->config.max_message_bytes;
    source.scheduler_capacity = owner->config.scheduler_capacity;
    source.scheduler_max_steps_per_poll = owner->config.scheduler_max_steps_per_poll;
    source.first_message_id = owner->config.first_message_id;
    rc = turbo_flow_cnet_listener_source_open_managed(&source, stage,
                                                      &owner->handle.listener_source);
    break;
  }
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE: {
    turbo_flow_cnet_packet_source_config_t source = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
    source.flow = flow;
    source.source_name = owner->source_name;
    source.endpoint = &owner->config.endpoint;
    source.content = &owner->config.content;
    source.queue_capacity = owner->config.queue_capacity;
    source.max_message_bytes = owner->config.max_message_bytes;
    source.scheduler_capacity = owner->config.scheduler_capacity;
    source.scheduler_max_steps_per_poll = owner->config.scheduler_max_steps_per_poll;
    source.first_message_id = owner->config.first_message_id;
    rc = turbo_flow_cnet_packet_source_open_managed(&source, stage, &owner->handle.packet_source);
    break;
  }
  default:
    break;
  }
  owner->last_status = rc;
  owner->initial_demand_pending = rc == SALTS_OK;
  owner->state =
      rc == SALTS_OK ? TURBO_FLOW_MANAGED_BOUNDARY_RUNNING : TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  return rc;
}

static int cnet_plugin_source_stop_handle(cnet_plugin_owner_t *owner, uint32_t timeout_ms) {
  int rc = SALTS_OK;
  if (!owner || !owner->handle.any) return SALTS_OK;
  switch (owner->config.kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE:
    rc = turbo_flow_cnet_stream_source_stop(owner->handle.stream_source, timeout_ms);
    break;
  case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE:
    rc = turbo_flow_cnet_listener_source_stop(owner->handle.listener_source, timeout_ms);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE:
    rc = turbo_flow_cnet_packet_source_stop(owner->handle.packet_source, timeout_ms);
    break;
  default:
    return SALTS_EINVAL;
  }
  return rc == SALTS_EALREADY ? SALTS_OK : rc;
}

static void cnet_plugin_source_stop(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  int rc;
  (void)stage;
  if (!owner) return;
  owner->state = TURBO_FLOW_MANAGED_BOUNDARY_STOPPING;
  rc = cnet_plugin_source_stop_handle(owner, owner->config.stop_timeout_ms);
  owner->last_status = rc;
  owner->state =
      rc == SALTS_OK ? TURBO_FLOW_MANAGED_BOUNDARY_STOPPED : TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  if (rc != SALTS_OK) (void)turbo_flow_adapter_report_stop_status(flow, rc);
}

static void cnet_plugin_source_shutdown(void *ctx) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner) return;
  if (owner->state != TURBO_FLOW_MANAGED_BOUNDARY_STOPPED && owner->handle.any) {
    int rc = cnet_plugin_source_stop_handle(owner, owner->config.stop_timeout_ms);
    owner->last_status = rc;
    owner->state =
        rc == SALTS_OK ? TURBO_FLOW_MANAGED_BOUNDARY_STOPPED : TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  }
}

static int cnet_plugin_register_source(cnet_plugin_owner_t *owner, turbo_flow_t *flow) {
  turbo_flow_adapter_ops_t ops = {0};
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_managed_boundary_provider_ops_t boundary =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_source_registration_t registration =
      TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_INIT;
  ops.start = cnet_plugin_source_start;
  ops.stop = cnet_plugin_source_stop;
  ops.shutdown = cnet_plugin_source_shutdown;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
  schema.direction = TURBO_FLOW_ADAPTER_INPUT;
  boundary.resource.metadata = cnet_plugin_boundary_metadata;
  boundary.descriptor = cnet_plugin_boundary_descriptor;
  boundary.snapshot = cnet_plugin_boundary_snapshot;
  registration.adapter_name = owner->name;
  registration.adapter_ops = &ops;
  registration.schema = &schema;
  registration.owner_name = owner->name;
  registration.boundary_ops = &boundary;
  registration.ctx = owner;
  return turbo_flow_register_managed_source_adapter(flow, &registration);
}

static int cnet_plugin_register_sink(cnet_plugin_owner_t *owner, turbo_flow_t *flow) {
  switch (owner->config.kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SINK: {
    turbo_flow_cnet_stream_sink_config_t sink = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    sink.flow = flow;
    sink.adapter_name = owner->name;
    sink.uri = owner->config.uri;
    sink.client = &owner->config.client;
    sink.socket_options = &owner->config.socket_options;
    sink.tls = owner->config.tls_enabled ? &owner->config.tls_client : NULL;
    sink.max_message_bytes = owner->config.max_message_bytes;
    sink.actor_command_capacity = owner->config.actor_command_capacity;
    sink.actor_max_steps_per_poll = owner->config.actor_max_steps_per_poll;
    sink.stop_timeout_ms = owner->config.stop_timeout_ms;
    return turbo_flow_cnet_stream_sink_register(&sink, &owner->handle.stream_sink);
  }
  case TURBO_FLOW_CNET_PLUGIN_DATAGRAM_SINK: {
    turbo_flow_cnet_datagram_sink_config_t sink = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
    sink.flow = flow;
    sink.adapter_name = owner->name;
    sink.datagram = &owner->config.datagram;
    sink.peer = owner->config.peer;
    sink.max_message_bytes = owner->config.max_message_bytes;
    sink.actor_command_capacity = owner->config.actor_command_capacity;
    sink.actor_max_steps_per_poll = owner->config.actor_max_steps_per_poll;
    sink.stop_timeout_ms = owner->config.stop_timeout_ms;
    return turbo_flow_cnet_datagram_sink_register(&sink, &owner->handle.datagram_sink);
  }
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SINK: {
    turbo_flow_cnet_packet_sink_config_t sink = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
    sink.flow = flow;
    sink.adapter_name = owner->name;
    sink.endpoint = &owner->config.endpoint;
    sink.peer = owner->config.peer;
    sink.conversation = owner->config.conversation;
    sink.send_capacity = owner->config.adapter_send_capacity;
    sink.max_message_bytes = owner->config.max_message_bytes;
    sink.actor_command_capacity = owner->config.actor_command_capacity;
    sink.actor_max_steps_per_poll = owner->config.actor_max_steps_per_poll;
    sink.stop_timeout_ms = owner->config.stop_timeout_ms;
    return turbo_flow_cnet_packet_sink_register(&sink, &owner->handle.packet_sink);
  }
  default:
    return SALTS_EINVAL;
  }
}

static int cnet_plugin_owner_poll(void *ctx, uint32_t timeout_ms) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  int rc = SALTS_OK;
  if (!owner || !owner->handle.any || owner->quiesced) return SALTS_ESHUTDOWN;
  if (owner->initial_demand_pending) {
    switch (owner->config.kind) {
    case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE:
      rc = turbo_flow_cnet_stream_source_request(owner->handle.stream_source,
                                                 owner->config.initial_demand);
      break;
    case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE:
      rc = turbo_flow_cnet_listener_source_request(owner->handle.listener_source,
                                                   owner->config.initial_demand);
      break;
    case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE:
      rc = turbo_flow_cnet_packet_source_request(owner->handle.packet_source,
                                                 owner->config.initial_demand);
      break;
    default:
      owner->initial_demand_pending = 0;
      break;
    }
    if (rc != SALTS_OK) goto failed;
    owner->initial_demand_pending = 0;
  }
  switch (owner->config.kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE:
    rc = turbo_flow_cnet_stream_source_poll(owner->handle.stream_source, timeout_ms, NULL);
    break;
  case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE:
    rc = turbo_flow_cnet_listener_source_poll(owner->handle.listener_source, timeout_ms, NULL);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE:
    rc = turbo_flow_cnet_packet_source_poll(owner->handle.packet_source, timeout_ms, NULL);
    break;
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SINK:
    rc = turbo_flow_cnet_stream_sink_poll(owner->handle.stream_sink, timeout_ms, NULL);
    break;
  case TURBO_FLOW_CNET_PLUGIN_DATAGRAM_SINK:
    rc = turbo_flow_cnet_datagram_sink_poll(owner->handle.datagram_sink, timeout_ms, NULL);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SINK:
    rc = turbo_flow_cnet_packet_sink_poll(owner->handle.packet_sink, timeout_ms, NULL);
    break;
  default:
    rc = SALTS_EINVAL;
    break;
  }
  if (rc == SALTS_OK) return SALTS_OK;
failed:
  owner->last_status = rc;
  owner->state = TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  return rc;
}

static int cnet_plugin_owner_quiesce(void *ctx, uint64_t timeout_ms) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  owner->quiesced = 1;
  if (owner->state == TURBO_FLOW_MANAGED_BOUNDARY_RUNNING)
    owner->state = TURBO_FLOW_MANAGED_BOUNDARY_QUIESCING;
  return SALTS_OK;
}

static int cnet_plugin_owner_drain(void *ctx, uint64_t timeout_ms) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  if (owner->config.kind <= TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE && owner->handle.any &&
      owner->state != TURBO_FLOW_MANAGED_BOUNDARY_STOPPED)
    return owner->last_status == SALTS_OK ? SALTS_EBUSY : owner->last_status;
  return SALTS_OK;
}

static int cnet_plugin_owner_shutdown(void *ctx) { return ctx ? SALTS_OK : SALTS_EINVAL; }

static void cnet_plugin_owner_destroy(void *ctx) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  const turbo_flow_plugin_host_v1_t *host;
  if (!owner) return;
  host = owner->root->host;
  switch (owner->config.kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE:
    if (owner->handle.stream_source)
      (void)turbo_flow_cnet_stream_source_destroy(owner->handle.stream_source);
    break;
  case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE:
    if (owner->handle.listener_source)
      (void)turbo_flow_cnet_listener_source_destroy(owner->handle.listener_source);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE:
    if (owner->handle.packet_source)
      (void)turbo_flow_cnet_packet_source_destroy(owner->handle.packet_source);
    break;
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SINK:
    if (owner->handle.stream_sink)
      (void)turbo_flow_cnet_stream_sink_destroy(owner->handle.stream_sink);
    break;
  case TURBO_FLOW_CNET_PLUGIN_DATAGRAM_SINK:
    if (owner->handle.datagram_sink)
      (void)turbo_flow_cnet_datagram_sink_destroy(owner->handle.datagram_sink);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SINK:
    if (owner->handle.packet_sink)
      (void)turbo_flow_cnet_packet_sink_destroy(owner->handle.packet_sink);
    break;
  default:
    break;
  }
  cnet_plugin_root_remove_owner(owner->root, owner);
  memset(owner, 0, sizeof(*owner));
  host->deallocate(host->ctx, owner);
}

static int cnet_plugin_preflight(void *ctx, const turbo_flow_resolved_config_t *resolved,
                                 const char *name, turbo_flow_config_error_t *error) {
  const cnet_plugin_provider_t *provider = (const cnet_plugin_provider_t *)ctx;
  turbo_flow_cnet_plugin_config_t config;
  if (!provider || !provider->root || provider->root->quiesced)
    return cnet_plugin_error(error, SALTS_ESHUTDOWN, name, "preflight",
                             "CNet provider is quiesced");
  return turbo_flow_cnet_plugin_config_read(resolved, name, provider->kind, &config, error);
}

static int cnet_plugin_materialize(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_resolved_config_t *resolved, const char *name,
                                   turbo_flow_plugin_product_owner_v1_t *owner_out,
                                   turbo_flow_config_error_t *error) {
  cnet_plugin_provider_t *provider = (cnet_plugin_provider_t *)ctx;
  cnet_plugin_owner_t *owner;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  int rc;
  if (!provider || !provider->root || !flow || !resolved || !name || !name[0] || !owner_out ||
      owner_out->size < sizeof(*owner_out))
    return cnet_plugin_error(error, SALTS_EINVAL, name, "materialize",
                             "invalid CNet materialization arguments");
  rc = cnet_plugin_reference_validate(flow, name, provider->kind, error);
  if (rc != SALTS_OK) return rc;
  if (strlen(name) >= TURBO_FLOW_CNET_PLUGIN_NAME_CAPACITY)
    return cnet_plugin_error(error, SALTS_ERANGE, name, "name", "CNet adapter name is too long");
  if (vec_size(&provider->root->owners) >= TURBO_FLOW_CNET_PLUGIN_MAX_OWNERS)
    return cnet_plugin_error(error, SALTS_ENOSPC, name, "capacity",
                             "CNet plugin owner capacity is exhausted");
  owner = (cnet_plugin_owner_t *)provider->root->host->allocate(provider->root->host->ctx,
                                                                sizeof(*owner));
  if (!owner)
    return cnet_plugin_error(error, SALTS_ENOMEM, name, "allocate",
                             "failed to allocate CNet Product owner");
  memset(owner, 0, sizeof(*owner));
  owner->root = provider->root;
  memcpy(owner->name, name, strlen(name) + 1u);
  owner->state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  owner->last_status = SALTS_OK;
  rc = turbo_flow_cnet_plugin_config_read(resolved, name, provider->kind, &owner->config, error);
  if (rc != SALTS_OK) goto fail;
  owner->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  owner->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  owner->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  (void)snprintf(owner->metadata.uid, sizeof(owner->metadata.uid), "cnet:%s", name);
  (void)snprintf(owner->metadata.owner_name, sizeof(owner->metadata.owner_name), "%s", name);
  owner->metadata.generation = 1u;
  owner->metadata.observed_generation = 1u;
  owner->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  owner->descriptor.domain = owner->metadata.domain;
  owner->descriptor.kind = owner->metadata.kind;
  (void)snprintf(owner->descriptor.uid, sizeof(owner->descriptor.uid), "%s", owner->metadata.uid);
  (void)snprintf(owner->descriptor.owner_name, sizeof(owner->descriptor.owner_name), "%s", name);
  owner->descriptor.role_flags = provider->kind <= TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE
                                     ? TURBO_FLOW_MANAGED_BOUNDARY_SOURCE
                                     : TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  if (provider->kind <= TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE) {
    owner->descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE;
    owner->descriptor.output = owner->config.content;
  } else {
    owner->descriptor.input = owner->config.content;
  }
  rc = cnet_plugin_root_add_owner(provider->root, owner);
  if (rc != SALTS_OK) goto fail;
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD |
                     TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  descriptor.ctx = owner;
  descriptor.quiesce = cnet_plugin_owner_quiesce;
  descriptor.drain = cnet_plugin_owner_drain;
  descriptor.shutdown = cnet_plugin_owner_shutdown;
  descriptor.destroy = cnet_plugin_owner_destroy;
  descriptor.poll = cnet_plugin_owner_poll;
  rc = turbo_flow_plugin_product_owner_publish(owner_out, &descriptor);
  if (rc != SALTS_OK) {
    cnet_plugin_root_remove_owner(provider->root, owner);
    goto fail;
  }
  rc = provider->kind <= TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE
           ? cnet_plugin_register_source(owner, flow)
           : cnet_plugin_register_sink(owner, flow);
  if (rc == SALTS_OK) return SALTS_OK;
  *owner_out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  cnet_plugin_root_remove_owner(provider->root, owner);
fail:
  memset(owner, 0, sizeof(*owner));
  provider->root->host->deallocate(provider->root->host->ctx, owner);
  return cnet_plugin_error(error, rc, name, "materialize",
                           "failed to materialize CNet adapter owner");
}

static int cnet_plugin_load(const turbo_flow_plugin_host_v1_t *host, void **plugin_out) {
  cnet_plugin_root_t *root;
  int rc;
  if (plugin_out) *plugin_out = NULL;
  if (!host || host->size < sizeof(*host) || !host->allocate || !host->deallocate || !plugin_out)
    return SALTS_EINVAL;
  root = (cnet_plugin_root_t *)host->allocate(host->ctx, sizeof(*root));
  if (!root) return SALTS_ENOMEM;
  memset(root, 0, sizeof(*root));
  root->host = host;
  rc = cnet_plugin_stl_status(vec_init_bytes(&root->owners, sizeof(cnet_plugin_owner_t *),
                                             _Alignof(cnet_plugin_owner_t *),
                                             TURBO_FLOW_CNET_PLUGIN_MAX_OWNERS));
  if (rc == SALTS_OK)
    rc = cnet_plugin_stl_status(vec_reserve(&root->owners, TURBO_FLOW_CNET_PLUGIN_MAX_OWNERS));
  if (rc != SALTS_OK) {
    vec_destroy(&root->owners);
    host->deallocate(host->ctx, root);
    return rc;
  }
  for (size_t i = 0u; i < TURBO_FLOW_CNET_PLUGIN_KIND_COUNT; ++i) {
    root->providers[i].root = root;
    root->providers[i].kind = (turbo_flow_cnet_plugin_kind_t)i;
  }
  *plugin_out = root;
  return SALTS_OK;
}

static int cnet_plugin_register(void *plugin,
                                const turbo_flow_plugin_registration_v1_t *registration) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)plugin;
  if (!root || !registration || registration->size < sizeof(*registration) ||
      !registration->add_transactional_adapter_provider)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < TURBO_FLOW_CNET_PLUGIN_KIND_COUNT; ++i) {
    turbo_flow_plugin_transactional_adapter_provider_v1_t provider =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT;
    int rc;
    provider.kind = turbo_flow_cnet_plugin_kind_name((turbo_flow_cnet_plugin_kind_t)i);
    provider.ctx = &root->providers[i];
    provider.preflight = cnet_plugin_preflight;
    provider.materialize = cnet_plugin_materialize;
    rc = registration->add_transactional_adapter_provider(registration->ctx, &provider);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

static int cnet_plugin_quiesce(void *plugin, uint64_t timeout_ms) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)plugin;
  (void)timeout_ms;
  if (!root) return SALTS_EINVAL;
  if (vec_size(&root->owners) != 0u) return SALTS_EBUSY;
  root->quiesced = 1;
  return SALTS_OK;
}

static int cnet_plugin_shutdown(void *plugin) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)plugin;
  return root && root->quiesced && vec_size(&root->owners) == 0u ? SALTS_OK : SALTS_EBUSY;
}

static void cnet_plugin_destroy(void *plugin) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!root) return;
  host = root->host;
  vec_destroy(&root->owners);
  memset(root, 0, sizeof(*root));
  host->deallocate(host->ctx, root);
}

static const turbo_flow_plugin_api_v1_t cnet_plugin_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    "turbo-flow.cnet",
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL,
    cnet_plugin_load,
    cnet_plugin_register,
    cnet_plugin_quiesce,
    cnet_plugin_shutdown,
    cnet_plugin_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &cnet_plugin_api;
}
