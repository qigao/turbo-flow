#include "turbo_flow_cnet_provider_adapter_internal.h"
#include "turbo_flow_cnet_typed_config_internal.h"

#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  CNET_PROVIDER_NAME_CAPACITY = 256,
  CNET_PROVIDER_MAX_OWNERS = 256
};

#define CNET_PROVIDER_STREAM_SOURCE_ID "cnet.stream_source"
#define CNET_PROVIDER_LISTENER_SOURCE_ID "cnet.listener_source"
#define CNET_PROVIDER_PACKET_SOURCE_ID "cnet.packet_source"
#define CNET_PROVIDER_STREAM_SINK_ID "cnet.stream_sink"
#define CNET_PROVIDER_DATAGRAM_SINK_ID "cnet.datagram_sink"
#define CNET_PROVIDER_PACKET_SINK_ID "cnet.packet_sink"

typedef struct cnet_plugin_root_s cnet_plugin_root_t;
typedef struct cnet_plugin_owner_s cnet_plugin_owner_t;

typedef struct cnet_provider_slot_s {
  cnet_plugin_root_t *root;
  unsigned kind;
} cnet_provider_slot_t;

struct cnet_plugin_root_s {
  bool started;
  bool stopping;
  size_t owners;
  cnet_provider_slot_t slots[CNET_TYPED_KIND_COUNT];
};

struct cnet_plugin_owner_s {
  cnet_plugin_root_t *root;
  cnet_typed_runtime_config_t config;
  char name[CNET_PROVIDER_NAME_CAPACITY];
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

static cnet_plugin_root_t provider_root;
static turbo_flow_provider_factory provider_factories[CNET_TYPED_KIND_COUNT];
static salts_plugin_export provider_exports[CNET_TYPED_KIND_COUNT];
static salts_once_t provider_once = SALTS_ONCE_INIT;

static const char *provider_identity(unsigned kind) {
  switch (kind) {
    case CNET_TYPED_STREAM_SOURCE: return CNET_PROVIDER_STREAM_SOURCE_ID;
    case CNET_TYPED_LISTENER_SOURCE: return CNET_PROVIDER_LISTENER_SOURCE_ID;
    case CNET_TYPED_PACKET_SOURCE: return CNET_PROVIDER_PACKET_SOURCE_ID;
    case CNET_TYPED_STREAM_SINK: return CNET_PROVIDER_STREAM_SINK_ID;
    case CNET_TYPED_DATAGRAM_SINK: return CNET_PROVIDER_DATAGRAM_SINK_ID;
    case CNET_TYPED_PACKET_SINK: return CNET_PROVIDER_PACKET_SINK_ID;
    default: return NULL;
  }
}

static int cnet_plugin_root_add_owner(
    cnet_plugin_root_t *root, cnet_plugin_owner_t *owner) {
  if (!root || !owner || !root->started || root->stopping)
    return SALTS_ESHUTDOWN;
  if (root->owners >= CNET_PROVIDER_MAX_OWNERS) return SALTS_ENOSPC;
  ++root->owners;
  return SALTS_OK;
}

static void cnet_plugin_root_remove_owner(
    cnet_plugin_root_t *root, const cnet_plugin_owner_t *owner) {
  (void)owner;
  if (root && root->owners != 0u) --root->owners;
}

static int cnet_plugin_error(turbo_flow_config_error_t *error, int status, const char *name,
                             const char *phase, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.stages.%s.%s", name ? name : "unknown",
                   phase ? phase : "provider");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int cnet_plugin_root_add_owner(cnet_plugin_root_t *root, cnet_plugin_owner_t *owner) {
  if (!root || !owner || root->quiesced) return SALTS_ESHUTDOWN;
  if (vec_size(&root->owners) >= CNET_PROVIDER_MAX_OWNERS) return SALTS_ENOSPC;
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

static int cnet_plugin_reference_validate(
    const turbo_flow_t *flow, const char *instance_name, unsigned kind,
    turbo_flow_config_error_t *error) {
  const turbo_flow_stage_plan_t *stage;
  const char *identity;
  int stage_index;
  const int expected_source = kind <= CNET_TYPED_PACKET_SOURCE;

  if (!flow || !instance_name || !instance_name[0])
    return SALTS_EINVAL;
  identity = provider_identity(kind);
  if (!identity) return SALTS_EINVAL;
  stage_index = turbo_flow_find_stage(flow, instance_name);
  if (stage_index < 0)
    return cnet_plugin_error(
        error, SALTS_ENOENT, instance_name, "instance",
        "CNet provider stage instance is missing");
  stage = turbo_flow_stage_at(flow, (size_t)stage_index);
  if (!stage || !stage->adapter_name ||
      strcmp(stage->adapter_name, identity) != 0)
    return cnet_plugin_error(
        error, SALTS_EPROTO, instance_name, "provider",
        "CNet stage provider identity does not match factory export");
  if ((stage->is_source != 0) != expected_source)
    return cnet_plugin_error(
        error, SALTS_EINVAL, instance_name, "role",
        "CNet provider kind does not match the Graph stage role");
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
  case CNET_TYPED_STREAM_SOURCE:
    if (owner->handle.stream_source) {
      turbo_flow_cnet_stream_source_snapshot_t source = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
      rc = turbo_flow_cnet_stream_source_snapshot(owner->handle.stream_source, &source);
      snapshot.demand = source.outstanding_demand;
      snapshot.in_flight = source.receive_pending != 0;
      snapshot.accepted = source.messages_received;
      snapshot.last_status = source.status;
    }
    break;
  case CNET_TYPED_LISTENER_SOURCE:
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
  case CNET_TYPED_PACKET_SOURCE:
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

static int cnet_plugin_endpoint_format(char *out, size_t capacity, const char *scheme,
                                       const char *host, uint16_t port) {
  int written;
  if (!out || capacity == 0u || !scheme || !scheme[0] || !host || !host[0]) return SALTS_EINVAL;
  written = strchr(host, ':')
                ? snprintf(out, capacity, "%s://[%s]:%u", scheme, host, (unsigned)port)
                : snprintf(out, capacity, "%s://%s:%u", scheme, host, (unsigned)port);
  if (written < 0 || (size_t)written >= capacity) return SALTS_ERANGE;
  return SALTS_OK;
}

static turbo_flow_connection_state_t
cnet_plugin_source_connection_state(const cnet_plugin_owner_t *owner) {
  if (!owner) return TURBO_FLOW_CONNECTION_FAILED;
  switch (owner->state) {
  case TURBO_FLOW_MANAGED_BOUNDARY_STARTING:
    return TURBO_FLOW_CONNECTION_CONNECTING;
  case TURBO_FLOW_MANAGED_BOUNDARY_RUNNING:
    return TURBO_FLOW_CONNECTION_READY;
  case TURBO_FLOW_MANAGED_BOUNDARY_QUIESCING:
  case TURBO_FLOW_MANAGED_BOUNDARY_DRAINING:
  case TURBO_FLOW_MANAGED_BOUNDARY_STOPPING:
    return TURBO_FLOW_CONNECTION_CLOSING;
  case TURBO_FLOW_MANAGED_BOUNDARY_FAILED:
    return TURBO_FLOW_CONNECTION_FAILED;
  default:
    return TURBO_FLOW_CONNECTION_STOPPED;
  }
}

static int cnet_plugin_source_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  turbo_flow_connection_snapshot_t snapshot;
  int rc;
  if (!owner || !out) return SALTS_EINVAL;
  memset(&snapshot, 0, sizeof(snapshot));
  snapshot.adapter_name = owner->name;
  snapshot.adapter_kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  snapshot.direction = TURBO_FLOW_ADAPTER_INPUT;
  snapshot.state = cnet_plugin_source_connection_state(owner);
  snapshot.last_status = owner->last_status;

  if (owner->config.kind == CNET_TYPED_LISTENER_SOURCE) {
    turbo_flow_cnet_listener_source_snapshot_t source =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    uint16_t port = owner->config.listener.port;
    snapshot.connection_limit = (uint64_t)owner->config.max_connections;
    if (owner->handle.listener_source) {
      rc = turbo_flow_cnet_listener_source_snapshot(owner->handle.listener_source, &source);
      if (rc != SALTS_OK) return rc;
      port = source.bound_port;
      snapshot.connections_current = (uint64_t)source.active_connections;
      snapshot.in_flight_messages = source.receive_pending ? 1u : 0u;
      snapshot.last_status = source.status;
    }
    rc = cnet_plugin_endpoint_format(snapshot.endpoint, sizeof(snapshot.endpoint), "tcp",
                                     owner->config.listener.host, port);
    if (rc != SALTS_OK) return rc;
    *out = snapshot;
    return SALTS_OK;
  }

  if (owner->config.kind == CNET_TYPED_PACKET_SOURCE) {
    turbo_flow_cnet_packet_source_snapshot_t source = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    uint16_t port = owner->config.endpoint.datagram.port;
    const char *scheme = owner->config.endpoint.protocol == CNET_PACKET_UDP ? "udp" : "kcp";
    snapshot.connection_limit = (uint64_t)owner->config.endpoint.session_capacity;
    if (owner->handle.packet_source) {
      rc = turbo_flow_cnet_packet_source_snapshot(owner->handle.packet_source, &source);
      if (rc != SALTS_OK) return rc;
      port = source.bound_port;
      snapshot.connections_current = source.sessions_opened >= source.sessions_closed
                                         ? source.sessions_opened - source.sessions_closed
                                         : 0u;
      snapshot.in_flight_messages = (uint64_t)source.queue_depth;
      snapshot.last_status = source.status;
    }
    rc = cnet_plugin_endpoint_format(snapshot.endpoint, sizeof(snapshot.endpoint), scheme,
                                     owner->config.endpoint.datagram.host, port);
    if (rc != SALTS_OK) return rc;
    *out = snapshot;
    return SALTS_OK;
  }

  return SALTS_ENOTSUP;
}

static int cnet_plugin_source_start(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  int rc = SALTS_EINVAL;
  if (!owner || !flow || !stage || owner->handle.any || owner->quiesced)
    return SALTS_EINVAL;
  if (!stage->name || strcmp(stage->name, owner->name) != 0)
    return SALTS_EPROTO;
  owner->state = TURBO_FLOW_MANAGED_BOUNDARY_STARTING;
  switch (owner->config.kind) {
  case CNET_TYPED_STREAM_SOURCE: {
    turbo_flow_cnet_stream_source_config_t source = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
    source.flow = flow;
    source.source_name = owner->name;
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
  case CNET_TYPED_LISTENER_SOURCE: {
    turbo_flow_cnet_listener_source_config_t source = TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
    source.flow = flow;
    source.source_name = owner->name;
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
  case CNET_TYPED_PACKET_SOURCE: {
    turbo_flow_cnet_packet_source_config_t source = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
    source.flow = flow;
    source.source_name = owner->name;
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
  case CNET_TYPED_STREAM_SOURCE:
    rc = turbo_flow_cnet_stream_source_stop(owner->handle.stream_source, timeout_ms);
    break;
  case CNET_TYPED_LISTENER_SOURCE:
    rc = turbo_flow_cnet_listener_source_stop(owner->handle.listener_source, timeout_ms);
    break;
  case CNET_TYPED_PACKET_SOURCE:
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

_Static_assert(sizeof(cnet_connection) <= TURBO_FLOW_TRANSPORT_REPLY_SESSION_BYTES,
               "CNet connection must fit generic reply token");
_Static_assert(sizeof(cnet_packet_session) <= TURBO_FLOW_TRANSPORT_REPLY_SESSION_BYTES,
               "CNet packet session must fit generic reply token");

static int cnet_plugin_reply_session_pack(turbo_flow_transport_reply_session_t *out,
                                          const void *value, size_t value_size) {
  if (!out || !value || value_size == 0u ||
      value_size > TURBO_FLOW_TRANSPORT_REPLY_SESSION_BYTES)
    return SALTS_EINVAL;
  *out = (turbo_flow_transport_reply_session_t)TURBO_FLOW_TRANSPORT_REPLY_SESSION_INIT;
  out->token_size = value_size;
  memcpy(out->token, value, value_size);
  return SALTS_OK;
}

static int cnet_plugin_reply_session_unpack(
    const turbo_flow_transport_reply_session_t *session, void *out, size_t value_size) {
  if (!session || !out || session->size != sizeof(*session) ||
      session->version != TURBO_FLOW_TRANSPORT_REPLY_API_VERSION ||
      session->token_size != value_size)
    return SALTS_EINVAL;
  memcpy(out, session->token, value_size);
  return SALTS_OK;
}

static int cnet_plugin_transport_reply_capture(
    void *ctx, const turbo_flow_msg_t *message, turbo_flow_transport_reply_session_t *session) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner || !message || !session) return SALTS_EINVAL;
  switch (owner->config.kind) {
  case CNET_TYPED_LISTENER_SOURCE: {
    const turbo_flow_cnet_listener_message_context_t *transport =
        turbo_flow_cnet_listener_message_context(message);
    return transport
               ? cnet_plugin_reply_session_pack(session, &transport->connection,
                                                sizeof(transport->connection))
               : SALTS_ENOENT;
  }
  case CNET_TYPED_PACKET_SOURCE: {
    const turbo_flow_cnet_packet_message_context_t *transport =
        turbo_flow_cnet_packet_message_context(message);
    return transport
               ? cnet_plugin_reply_session_pack(session, &transport->session,
                                                sizeof(transport->session))
               : SALTS_ENOENT;
  }
  default:
    return SALTS_ENOTSUP;
  }
}

static int cnet_plugin_transport_reply_send(
    void *ctx, const turbo_flow_transport_reply_request_t *request) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner || !request) return SALTS_EINVAL;
  switch (owner->config.kind) {
  case CNET_TYPED_LISTENER_SOURCE: {
    cnet_connection connection = {0};
    turbo_flow_cnet_listener_reply_request_t cnet_request =
        TURBO_FLOW_CNET_LISTENER_REPLY_REQUEST_INIT;
    int rc = cnet_plugin_reply_session_unpack(&request->session, &connection, sizeof(connection));
    if (rc != SALTS_OK) return rc;
    cnet_request.connection = connection;
    cnet_request.data = request->data;
    cnet_request.data_size = request->data_size;
    cnet_request.tag = request->tag;
    return owner->handle.listener_source
               ? turbo_flow_cnet_listener_source_reply_send(owner->handle.listener_source,
                                                            &cnet_request)
               : SALTS_EBUSY;
  }
  case CNET_TYPED_PACKET_SOURCE: {
    cnet_packet_session session = {0};
    int rc = cnet_plugin_reply_session_unpack(&request->session, &session, sizeof(session));
    if (rc != SALTS_OK) return rc;
    return owner->handle.packet_source
               ? turbo_flow_cnet_packet_source_reply_send(owner->handle.packet_source, session,
                                                          request->data, request->data_size,
                                                          request->tag)
               : SALTS_EBUSY;
  }
  default:
    return SALTS_ENOTSUP;
  }
}

static int cnet_plugin_transport_reply_send_slices(
    void *ctx, const turbo_flow_transport_reply_slices_request_t *request) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  cnet_connection connection = {0};
  turbo_flow_cnet_listener_reply_slices_request_t cnet_request =
      TURBO_FLOW_CNET_LISTENER_REPLY_SLICES_REQUEST_INIT;
  int rc;
  if (!owner || !request) return SALTS_EINVAL;
  if (owner->config.kind != CNET_TYPED_LISTENER_SOURCE)
    return SALTS_ENOTSUP;
  rc = cnet_plugin_reply_session_unpack(&request->session, &connection, sizeof(connection));
  if (rc != SALTS_OK) return rc;
  cnet_request.connection = connection;
  cnet_request.segments = request->segments;
  cnet_request.segment_count = request->segment_count;
  cnet_request.tag = request->tag;
  return owner->handle.listener_source
             ? turbo_flow_cnet_listener_source_reply_send_slices(
                   owner->handle.listener_source, &cnet_request)
             : SALTS_EBUSY;
}

static int cnet_plugin_transport_reply_take_terminal(
    void *ctx, turbo_flow_transport_reply_terminal_t *terminal) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner || !terminal) return SALTS_EINVAL;
  switch (owner->config.kind) {
  case CNET_TYPED_LISTENER_SOURCE: {
    turbo_flow_cnet_listener_reply_terminal_t cnet_terminal =
        TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_INIT;
    int rc;
    if (!owner->handle.listener_source) return SALTS_EBUSY;
    rc = turbo_flow_cnet_listener_source_reply_take_terminal(owner->handle.listener_source,
                                                              &cnet_terminal);
    if (rc != SALTS_OK) return rc;
    *terminal = (turbo_flow_transport_reply_terminal_t)TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_INIT;
    rc = cnet_plugin_reply_session_pack(&terminal->session, &cnet_terminal.connection,
                                        sizeof(cnet_terminal.connection));
    if (rc != SALTS_OK) return rc;
    terminal->data_size = cnet_terminal.data_size;
    terminal->status = cnet_terminal.status;
    terminal->tag = cnet_terminal.tag;
    switch (cnet_terminal.kind) {
    case TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_SENT:
      terminal->kind = TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_SENT;
      break;
    case TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_PEER_CLOSED:
      terminal->kind = TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_PEER_CLOSED;
      break;
    case TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_STOPPED:
      terminal->kind = TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_STOPPED;
      break;
    case TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_PEER_FAILED:
      terminal->kind = TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_FAILED;
      break;
    default:
      return SALTS_EPROTO;
    }
    return SALTS_OK;
  }
  case CNET_TYPED_PACKET_SOURCE: {
    turbo_flow_cnet_packet_reply_terminal_t cnet_terminal =
        TURBO_FLOW_CNET_PACKET_REPLY_TERMINAL_INIT;
    int rc;
    if (!owner->handle.packet_source) return SALTS_EBUSY;
    rc = turbo_flow_cnet_packet_source_reply_take_terminal(owner->handle.packet_source,
                                                            &cnet_terminal);
    if (rc != SALTS_OK) return rc;
    *terminal = (turbo_flow_transport_reply_terminal_t)TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_INIT;
    rc = cnet_plugin_reply_session_pack(&terminal->session, &cnet_terminal.session,
                                        sizeof(cnet_terminal.session));
    if (rc != SALTS_OK) return rc;
    terminal->kind = cnet_terminal.status == SALTS_OK
                         ? TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_SENT
                         : (cnet_terminal.status == SALTS_ECANCELED
                                ? TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_STOPPED
                                : TURBO_FLOW_TRANSPORT_REPLY_TERMINAL_FAILED);
    terminal->data_size = cnet_terminal.data_size;
    terminal->status = cnet_terminal.status;
    terminal->tag = cnet_terminal.tag;
    return SALTS_OK;
  }
  default:
    return SALTS_ENOTSUP;
  }
}

static int cnet_plugin_register_source(
    cnet_plugin_owner_t *owner, turbo_flow_t *flow) {
  turbo_flow_adapter_ops_t ops = {0};
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_managed_boundary_provider_ops_t boundary =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_provider_adapter_registration_v1_t registration =
      TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT;
  turbo_flow_transport_reply_provider_ops_t reply =
      TURBO_FLOW_TRANSPORT_REPLY_PROVIDER_OPS_INIT;
  const char *stages[1];
  const char *identity;
  int rc;

  if (!owner || !flow) return SALTS_EINVAL;
  identity = provider_identity(owner->config.kind);
  if (!identity) return SALTS_EINVAL;
  stages[0] = owner->name;

  ops.start = cnet_plugin_source_start;
  ops.stop = cnet_plugin_source_stop;
  ops.shutdown = cnet_plugin_source_shutdown;
  if (owner->config.kind == CNET_TYPED_LISTENER_SOURCE ||
      owner->config.kind == CNET_TYPED_PACKET_SOURCE)
    ops.connection_snapshot = cnet_plugin_source_connection_snapshot;

  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
  schema.direction = TURBO_FLOW_ADAPTER_INPUT;

  boundary.resource.metadata = cnet_plugin_boundary_metadata;
  boundary.descriptor = cnet_plugin_boundary_descriptor;
  boundary.snapshot = cnet_plugin_boundary_snapshot;

  registration.provider_identity = identity;
  registration.stage_names = stages;
  registration.stage_count = 1u;
  registration.adapter_ops = &ops;
  registration.schema = &schema;
  registration.managed_owner_name = owner->name;
  registration.managed_boundary_ops = &boundary;
  registration.ctx = owner;
  rc = turbo_flow_provider_adapter_register(flow, &registration);
  if (rc != SALTS_OK) return rc;

  if (owner->config.kind != CNET_TYPED_LISTENER_SOURCE &&
      owner->config.kind != CNET_TYPED_PACKET_SOURCE)
    return SALTS_OK;

  reply.capture = cnet_plugin_transport_reply_capture;
  reply.send = cnet_plugin_transport_reply_send;
  reply.take_terminal = cnet_plugin_transport_reply_take_terminal;
  if (owner->config.kind == CNET_TYPED_LISTENER_SOURCE)
    reply.send_slices = cnet_plugin_transport_reply_send_slices;
  return turbo_flow_provider_adapter_transport_reply_register(
      flow, owner->name, &reply, owner);
}

static int cnet_plugin_register_sink(
    cnet_plugin_owner_t *owner, turbo_flow_t *flow) {
  const char *stages[1];
  const char *identity;

  if (!owner || !flow) return SALTS_EINVAL;
  identity = provider_identity(owner->config.kind);
  if (!identity) return SALTS_EINVAL;
  stages[0] = owner->name;

  switch (owner->config.kind) {
  case CNET_TYPED_STREAM_SINK: {
    turbo_flow_cnet_stream_sink_config_t sink =
        TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    sink.flow = flow;
    sink.adapter_name = owner->name;
    sink.uri = owner->config.uri;
    sink.client = &owner->config.client;
    sink.socket_options = &owner->config.socket_options;
    sink.tls = owner->config.tls_enabled ? &owner->config.tls_client : NULL;
    sink.max_message_bytes = owner->config.max_message_bytes;
    sink.actor_command_capacity = owner->config.actor_command_capacity;
    sink.actor_max_steps_per_poll =
        owner->config.actor_max_steps_per_poll;
    sink.stop_timeout_ms = owner->config.stop_timeout_ms;
    return turbo_flow_cnet_stream_sink_register_provider(
        &sink, identity, stages, 1u, &owner->handle.stream_sink);
  }
  case CNET_TYPED_DATAGRAM_SINK: {
    turbo_flow_cnet_datagram_sink_config_t sink =
        TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
    sink.flow = flow;
    sink.adapter_name = owner->name;
    sink.datagram = &owner->config.datagram;
    sink.peer = owner->config.peer;
    sink.max_message_bytes = owner->config.max_message_bytes;
    sink.actor_command_capacity = owner->config.actor_command_capacity;
    sink.actor_max_steps_per_poll =
        owner->config.actor_max_steps_per_poll;
    sink.stop_timeout_ms = owner->config.stop_timeout_ms;
    return turbo_flow_cnet_datagram_sink_register_provider(
        &sink, identity, stages, 1u, &owner->handle.datagram_sink);
  }
  case CNET_TYPED_PACKET_SINK: {
    turbo_flow_cnet_packet_sink_config_t sink =
        TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
    sink.flow = flow;
    sink.adapter_name = owner->name;
    sink.endpoint = &owner->config.endpoint;
    sink.peer = owner->config.peer;
    sink.conversation = owner->config.conversation;
    sink.send_capacity = owner->config.adapter_send_capacity;
    sink.max_message_bytes = owner->config.max_message_bytes;
    sink.actor_command_capacity = owner->config.actor_command_capacity;
    sink.actor_max_steps_per_poll =
        owner->config.actor_max_steps_per_poll;
    sink.stop_timeout_ms = owner->config.stop_timeout_ms;
    return turbo_flow_cnet_packet_sink_register_provider(
        &sink, identity, stages, 1u, &owner->handle.packet_sink);
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
    case CNET_TYPED_STREAM_SOURCE:
      rc = turbo_flow_cnet_stream_source_request(owner->handle.stream_source,
                                                 owner->config.initial_demand);
      break;
    case CNET_TYPED_LISTENER_SOURCE:
      rc = turbo_flow_cnet_listener_source_request(owner->handle.listener_source,
                                                   owner->config.initial_demand);
      break;
    case CNET_TYPED_PACKET_SOURCE:
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
  case CNET_TYPED_STREAM_SOURCE:
    rc = turbo_flow_cnet_stream_source_poll(owner->handle.stream_source, timeout_ms, NULL);
    break;
  case CNET_TYPED_LISTENER_SOURCE:
    rc = turbo_flow_cnet_listener_source_poll(owner->handle.listener_source, timeout_ms, NULL);
    break;
  case CNET_TYPED_PACKET_SOURCE:
    rc = turbo_flow_cnet_packet_source_poll(owner->handle.packet_source, timeout_ms, NULL);
    break;
  case CNET_TYPED_STREAM_SINK:
    rc = turbo_flow_cnet_stream_sink_poll(owner->handle.stream_sink, timeout_ms, NULL);
    break;
  case CNET_TYPED_DATAGRAM_SINK:
    rc = turbo_flow_cnet_datagram_sink_poll(owner->handle.datagram_sink, timeout_ms, NULL);
    break;
  case CNET_TYPED_PACKET_SINK:
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
  int rc;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  if (owner->config.kind <= CNET_TYPED_PACKET_SOURCE && owner->handle.any &&
      owner->state != TURBO_FLOW_MANAGED_BOUNDARY_STOPPED)
    return owner->last_status == SALTS_OK ? SALTS_EBUSY : owner->last_status;

  /*
   * Reply-capable Sources retain authoritative terminal evidence after stop.
   * Destruction is the canonical quiescence probe: SALTS_EBUSY pins the Product
   * owner (and therefore the plugin generation) until the consumer drains those
   * terminals. Success transfers the handle to the drained state early so the
   * later void destroy callback cannot silently discard evidence.
   */
  if (owner->state == TURBO_FLOW_MANAGED_BOUNDARY_STOPPED &&
      owner->config.kind == CNET_TYPED_LISTENER_SOURCE &&
      owner->handle.listener_source) {
    rc = turbo_flow_cnet_listener_source_destroy(owner->handle.listener_source);
    if (rc != SALTS_OK) return rc;
    owner->handle.listener_source = NULL;
  } else if (owner->state == TURBO_FLOW_MANAGED_BOUNDARY_STOPPED &&
             owner->config.kind == CNET_TYPED_PACKET_SOURCE &&
             owner->handle.packet_source) {
    rc = turbo_flow_cnet_packet_source_destroy(owner->handle.packet_source);
    if (rc != SALTS_OK) return rc;
    owner->handle.packet_source = NULL;
  }
  return SALTS_OK;
}

static int cnet_plugin_owner_shutdown(void *ctx) { return ctx ? SALTS_OK : SALTS_EINVAL; }

static void cnet_plugin_owner_destroy(void *ctx) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  cnet_plugin_root_t *root;
  int rc = SALTS_OK;
  if (!owner) return;
  root = owner->root;
  switch (owner->config.kind) {
  case CNET_TYPED_STREAM_SOURCE:
    if (owner->handle.stream_source)
      rc = turbo_flow_cnet_stream_source_destroy(owner->handle.stream_source);
    break;
  case CNET_TYPED_LISTENER_SOURCE:
    if (owner->handle.listener_source)
      rc = turbo_flow_cnet_listener_source_destroy(owner->handle.listener_source);
    break;
  case CNET_TYPED_PACKET_SOURCE:
    if (owner->handle.packet_source)
      rc = turbo_flow_cnet_packet_source_destroy(owner->handle.packet_source);
    break;
  case CNET_TYPED_STREAM_SINK:
    if (owner->handle.stream_sink)
      rc = turbo_flow_cnet_stream_sink_destroy(owner->handle.stream_sink);
    break;
  case CNET_TYPED_DATAGRAM_SINK:
    if (owner->handle.datagram_sink)
      rc = turbo_flow_cnet_datagram_sink_destroy(owner->handle.datagram_sink);
    break;
  case CNET_TYPED_PACKET_SINK:
    if (owner->handle.packet_sink)
      rc = turbo_flow_cnet_packet_sink_destroy(owner->handle.packet_sink);
    break;
  default:
    rc = SALTS_EINVAL;
    break;
  }
  if (rc != SALTS_OK) return;
  cnet_plugin_root_remove_owner(root, owner);
  memset(owner, 0, sizeof(*owner));
  free(owner);
}

CMETA_IMPLEMENTS(
    turbo_flow_runtime_owner, cnet_runtime_owner,
    TURBO_FLOW_RUNTIME_OWNER_CONTROL_THREAD |
        TURBO_FLOW_RUNTIME_OWNER_EXTERNAL_POLL,
    .quiesce = cnet_plugin_owner_quiesce,
    .drain = cnet_plugin_owner_drain,
    .shutdown = cnet_plugin_owner_shutdown,
    .poll = cnet_plugin_owner_poll,
    .destroy = cnet_plugin_owner_destroy);

static const DataBindMessageNativeArtifact *provider_artifact(
    unsigned kind) {
  switch (kind) {
    case CNET_TYPED_STREAM_SOURCE:
      return CNetStreamSourceConfig_native_artifact();
    case CNET_TYPED_LISTENER_SOURCE:
      return CNetListenerSourceConfig_native_artifact();
    case CNET_TYPED_PACKET_SOURCE:
      return CNetPacketSourceConfig_native_artifact();
    case CNET_TYPED_STREAM_SINK:
      return CNetStreamSinkConfig_native_artifact();
    case CNET_TYPED_DATAGRAM_SINK:
      return CNetDatagramSinkConfig_native_artifact();
    case CNET_TYPED_PACKET_SINK:
      return CNetPacketSinkConfig_native_artifact();
    default:
      return NULL;
  }
}

static size_t provider_value_bytes(unsigned kind) {
  switch (kind) {
    case CNET_TYPED_STREAM_SOURCE: return sizeof(CNetStreamSourceConfig_t);
    case CNET_TYPED_LISTENER_SOURCE: return sizeof(CNetListenerSourceConfig_t);
    case CNET_TYPED_PACKET_SOURCE: return sizeof(CNetPacketSourceConfig_t);
    case CNET_TYPED_STREAM_SINK: return sizeof(CNetStreamSinkConfig_t);
    case CNET_TYPED_DATAGRAM_SINK: return sizeof(CNetDatagramSinkConfig_t);
    case CNET_TYPED_PACKET_SINK: return sizeof(CNetPacketSinkConfig_t);
    default: return 0u;
  }
}

static uint64_t provider_resource_capabilities(unsigned kind) {
  switch (kind) {
    case CNET_TYPED_STREAM_SOURCE:
    case CNET_TYPED_STREAM_SINK:
      return TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT;
    case CNET_TYPED_LISTENER_SOURCE:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    case CNET_TYPED_PACKET_SOURCE:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    case CNET_TYPED_DATAGRAM_SINK:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    case CNET_TYPED_PACKET_SINK:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    default:
      return 0u;
  }
}

static int instance_deployment(
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_cnet_deployment_view_t *deployment,
    turbo_flow_config_error_t *error) {
  turbo_flow_cnet_deployment_resource *resource;
  int rc;

  if (!instance || !deployment || !instance->resource ||
      instance->resource->size != sizeof(*instance->resource) ||
      !instance->resource->reference_name ||
      !instance->resource->reference_name[0] ||
      !instance->resource->identity ||
      !instance->resource->identity[0] ||
      !instance->resource->interface_desc ||
      !instance->resource->interface_value ||
      !cmeta_interface_desc_equal(
          instance->resource->interface_desc,
          turbo_flow_cnet_deployment_resource_interface()))
    return cnet_plugin_error(
        error, SALTS_EPROTO,
        instance ? instance->instance_name : NULL, "resource",
        "CNet provider requires an exact deployment resource Interface");

  resource = (turbo_flow_cnet_deployment_resource *)
      instance->resource->interface_value;
  if (!turbo_flow_cnet_deployment_resource_valid(resource))
    return cnet_plugin_error(
        error, SALTS_EPROTO, instance->instance_name, "resource",
        "CNet deployment resource handle is invalid");

  *deployment =
      (turbo_flow_cnet_deployment_view_t)
          TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  rc = turbo_flow_cnet_deployment_resource_snapshot(resource, deployment);
  if (rc != SALTS_OK)
    return cnet_plugin_error(
        error, rc, instance->instance_name, "resource",
        "CNet deployment resource snapshot failed");
  if (!turbo_flow_cnet_deployment_view_valid(deployment))
    return cnet_plugin_error(
        error, SALTS_EPROTO, instance->instance_name, "resource",
        "CNet deployment resource returned an invalid view");
  return SALTS_OK;
}

static int instance_runtime_config(
    const cnet_provider_slot_t *slot,
    const turbo_flow_provider_instance_v1_t *instance,
    cnet_typed_runtime_config_t *runtime,
    turbo_flow_config_error_t *error) {
  const DataBindMessageNativeArtifact *artifact;
  DataBindNativeTypeBinding native =
      DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
  DataBindError databind_error = DATA_BIND_ERROR_INIT;
  turbo_flow_cnet_deployment_view_t deployment =
      TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  size_t value_bytes;
  int rc;

  if (!slot || !instance || !runtime ||
      instance->size != sizeof(*instance) ||
      !instance->instance_name || !instance->instance_name[0] ||
      instance->config.size != sizeof(instance->config) ||
      !instance->config.type_name || !instance->config.data ||
      !instance->config.value)
    return cnet_plugin_error(
        error, SALTS_EINVAL,
        instance ? instance->instance_name : NULL, "config",
        "invalid CNet provider instance");

  artifact = provider_artifact(slot->kind);
  value_bytes = provider_value_bytes(slot->kind);
  if (!artifact || value_bytes == 0u ||
      instance->config.value_bytes != value_bytes ||
      strcmp(instance->config.type_name, artifact->type_name) != 0 ||
      artifact->native_binding(&native, &databind_error) != DATA_BIND_OK ||
      instance->config.data != native.data)
    return cnet_plugin_error(
        error, SALTS_EPROTO, instance->instance_name, "config",
        "CNet typed config does not match the provider contract");

  rc = instance_deployment(instance, &deployment, error);
  if (rc != SALTS_OK) return rc;

  switch (slot->kind) {
    case CNET_TYPED_STREAM_SOURCE:
      return cnet_typed_stream_source_config(
          (const CNetStreamSourceConfig_t *)instance->config.value,
          &deployment, instance->instance_name, runtime, error);
    case CNET_TYPED_LISTENER_SOURCE:
      return cnet_typed_listener_source_config(
          (const CNetListenerSourceConfig_t *)instance->config.value,
          &deployment, instance->instance_name, runtime, error);
    case CNET_TYPED_PACKET_SOURCE:
      return cnet_typed_packet_source_config(
          (const CNetPacketSourceConfig_t *)instance->config.value,
          &deployment, instance->instance_name, runtime, error);
    case CNET_TYPED_STREAM_SINK:
      return cnet_typed_stream_sink_config(
          (const CNetStreamSinkConfig_t *)instance->config.value,
          &deployment, instance->instance_name, runtime, error);
    case CNET_TYPED_DATAGRAM_SINK:
      return cnet_typed_datagram_sink_config(
          (const CNetDatagramSinkConfig_t *)instance->config.value,
          &deployment, instance->instance_name, runtime, error);
    case CNET_TYPED_PACKET_SINK:
      return cnet_typed_packet_sink_config(
          (const CNetPacketSinkConfig_t *)instance->config.value,
          &deployment, instance->instance_name, runtime, error);
    default:
      return cnet_plugin_error(
          error, SALTS_EINVAL, instance->instance_name, "provider",
          "unknown CNet provider identity");
  }
}

static int provider_contract(
    void *self, turbo_flow_provider_contract_v1_t *out) {
  cnet_provider_slot_t *slot = (cnet_provider_slot_t *)self;
  if (!slot || !slot->root || !out || out->size != sizeof(*out) ||
      !provider_artifact(slot->kind) ||
      provider_resource_capabilities(slot->kind) == 0u)
    return SALTS_EINVAL;

  *out =
      (turbo_flow_provider_contract_v1_t)
          TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  out->config.codec_factory = TurboFlowCNetProviderConfig_codec_create;
  out->config.message_artifact = provider_artifact(slot->kind);
  out->resource.contract_id =
      TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID;
  out->resource.contract_version =
      TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION;
  out->resource.required_capabilities =
      provider_resource_capabilities(slot->kind);
  out->resource.expected_interface =
      turbo_flow_cnet_deployment_resource_interface();
  return SALTS_OK;
}

static int provider_preflight(
    void *self, const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error) {
  cnet_provider_slot_t *slot = (cnet_provider_slot_t *)self;
  cnet_typed_runtime_config_t runtime;
  if (!slot || !slot->root || !slot->root->started ||
      slot->root->stopping)
    return SALTS_ESHUTDOWN;
  return instance_runtime_config(slot, instance, &runtime, error);
}

static int provider_materialize(
    void *self, turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error) {
  cnet_provider_slot_t *slot = (cnet_provider_slot_t *)self;
  cnet_plugin_owner_t *owner;
  const char *identity;
  int rc;

  if (!slot || !slot->root || !slot->root->started ||
      slot->root->stopping || !flow || !instance || !owner_out ||
      turbo_flow_runtime_owner_valid(owner_out))
    return SALTS_EINVAL;

  identity = provider_identity(slot->kind);
  if (!identity)
    return cnet_plugin_error(
        error, SALTS_EINVAL, instance->instance_name, "provider",
        "unknown CNet provider identity");
  rc = cnet_plugin_reference_validate(
      flow, instance->instance_name, slot->kind, error);
  if (rc != SALTS_OK) return rc;
  if (strlen(instance->instance_name) >= CNET_PROVIDER_NAME_CAPACITY)
    return cnet_plugin_error(
        error, SALTS_ERANGE, instance->instance_name, "name",
        "CNet stage instance name is too long");
  if (slot->root->owners >= CNET_PROVIDER_MAX_OWNERS)
    return cnet_plugin_error(
        error, SALTS_ENOSPC, instance->instance_name, "capacity",
        "CNet provider owner capacity is exhausted");

  owner = (cnet_plugin_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner)
    return cnet_plugin_error(
        error, SALTS_ENOMEM, instance->instance_name, "allocate",
        "failed to allocate CNet runtime owner");
  owner->root = slot->root;
  memcpy(
      owner->name, instance->instance_name,
      strlen(instance->instance_name) + 1u);
  owner->state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  owner->last_status = SALTS_OK;

  rc = instance_runtime_config(slot, instance, &owner->config, error);
  if (rc != SALTS_OK) goto fail;

  owner->metadata =
      (turbo_flow_resource_metadata_t)
          TURBO_FLOW_RESOURCE_METADATA_INIT;
  owner->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  owner->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  (void)snprintf(
      owner->metadata.uid, sizeof(owner->metadata.uid),
      "cnet:%s", owner->name);
  (void)snprintf(
      owner->metadata.owner_name, sizeof(owner->metadata.owner_name),
      "%s", owner->name);
  owner->metadata.generation = 1u;
  owner->metadata.observed_generation = 1u;

  owner->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)
          TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  owner->descriptor.domain = owner->metadata.domain;
  owner->descriptor.kind = owner->metadata.kind;
  (void)snprintf(
      owner->descriptor.uid, sizeof(owner->descriptor.uid),
      "%s", owner->metadata.uid);
  (void)snprintf(
      owner->descriptor.owner_name, sizeof(owner->descriptor.owner_name),
      "%s", owner->name);
  owner->descriptor.role_flags =
      slot->kind <= CNET_TYPED_PACKET_SOURCE
          ? TURBO_FLOW_MANAGED_BOUNDARY_SOURCE
          : TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  if (slot->kind <= CNET_TYPED_PACKET_SOURCE) {
    owner->descriptor.capability_flags =
        TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE;
    owner->descriptor.output = owner->config.content;
  } else {
    owner->descriptor.input = owner->config.content;
  }

  rc = slot->kind <= CNET_TYPED_PACKET_SOURCE
           ? cnet_plugin_register_source(owner, flow)
           : cnet_plugin_register_sink(owner, flow);
  if (rc != SALTS_OK) {
    cnet_plugin_error(
        error, rc, owner->name, "materialize",
        "failed to register CNet native adapter owner");
    goto fail;
  }

  rc = cnet_plugin_root_add_owner(slot->root, owner);
  if (rc != SALTS_OK) {
    cnet_plugin_error(
        error, rc, owner->name, "capacity",
        "failed to retain CNet runtime owner");
    goto fail_registered;
  }

  *owner_out = cnet_runtime_owner_as_turbo_flow_runtime_owner(owner);
  if (error)
    *error =
        (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  return SALTS_OK;

fail_registered:
  /*
   * This path is unreachable after the prechecked owner-capacity guard unless
   * provider stop races the control thread. Keep the Graph binding alive rather
   * than freeing callback storage behind it; materialization fails closed.
   */
  return rc;

fail:
  memset(owner, 0, sizeof(*owner));
  free(owner);
  return rc;
}

CMETA_IMPLEMENTS(
    turbo_flow_provider_factory, cnet_provider_factory_impl, 0u,
    .contract = provider_contract,
    .preflight = provider_preflight,
    .materialize = provider_materialize);

static void provider_init(void) {
  static const unsigned kinds[CNET_TYPED_KIND_COUNT] = {
      CNET_TYPED_STREAM_SOURCE,
      CNET_TYPED_LISTENER_SOURCE,
      CNET_TYPED_PACKET_SOURCE,
      CNET_TYPED_STREAM_SINK,
      CNET_TYPED_DATAGRAM_SINK,
      CNET_TYPED_PACKET_SINK};

  memset(&provider_root, 0, sizeof(provider_root));
  for (size_t i = 0u; i < CNET_TYPED_KIND_COUNT; ++i) {
    provider_root.slots[i].root = &provider_root;
    provider_root.slots[i].kind = kinds[i];
    provider_factories[i] =
        cnet_provider_factory_impl_as_turbo_flow_provider_factory(
            &provider_root.slots[i]);
    provider_exports[i] = (salts_plugin_export){
        .struct_size = SALTS_PLUGIN_EXPORT_SIZE,
        .kind = SALTS_PLUGIN_EXPORT_INTERFACE,
        .contract_version =
            TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_VERSION,
        .capabilities = 0u,
        .export_id = provider_identity(kinds[i]),
        .contract_id = TURBO_FLOW_PROVIDER_FACTORY_CONTRACT_ID,
        .value.interface = {
            .desc = turbo_flow_provider_factory_interface(),
            .value = &provider_factories[i],
        },
    };
  }
}

static salts_plugin_status SALTS_PLUGIN_CALL provider_start(void *self) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)self;
  if (!root) return SALTS_PLUGIN_INVALID_ARGUMENT;
  if (root->owners != 0u) return SALTS_PLUGIN_BUSY;
  root->stopping = false;
  root->started = true;
  return SALTS_PLUGIN_OK;
}

static salts_plugin_status SALTS_PLUGIN_CALL
provider_request_stop(void *self) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)self;
  if (!root) return SALTS_PLUGIN_INVALID_ARGUMENT;
  root->stopping = true;
  root->started = false;
  return SALTS_PLUGIN_OK;
}

static bool SALTS_PLUGIN_CALL provider_is_quiescent(const void *self) {
  const cnet_plugin_root_t *root =
      (const cnet_plugin_root_t *)self;
  return root && root->stopping && root->owners == 0u;
}

static void SALTS_PLUGIN_CALL provider_destroy(void *self) {
  cnet_plugin_root_t *root = (cnet_plugin_root_t *)self;
  if (!root || root->owners != 0u) return;
  root->started = false;
  root->stopping = true;
}

static salts_plugin_manifest provider_manifest = {
    .struct_size = SALTS_PLUGIN_MANIFEST_SIZE,
    .abi_version = SALTS_PLUGIN_ABI_VERSION,
    .plugin_id = "turbo-flow.cnet",
    .version = {2u, 0u, 0u},
    .self = &provider_root,
};

SALTS_PLUGIN_QUERY_EXPORT
const salts_plugin_manifest *SALTS_PLUGIN_CALL
salts_plugin_query(uint32_t host_abi) {
  if (host_abi != SALTS_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&provider_once, provider_init);
  provider_manifest.exports = provider_exports;
  provider_manifest.export_count = CNET_TYPED_KIND_COUNT;
  provider_manifest.start = provider_start;
  provider_manifest.request_stop = provider_request_stop;
  provider_manifest.is_quiescent = provider_is_quiescent;
  provider_manifest.destroy = provider_destroy;
  return &provider_manifest;
}
