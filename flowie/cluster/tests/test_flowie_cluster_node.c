#include "flowie_cluster_node_internal.h"
#include "flowie_cluster_endpoint_binding_internal.h"
#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct fake_cluster_component_s {
  char kind;
  uint32_t id;
} fake_cluster_component_t;

typedef struct fake_cluster_state_s {
  char events[4096];
  size_t event_size;
  uint32_t fail_shard_id;
  uint32_t fail_connector_start_id;
  uint32_t fail_connector_drain_once_id;
  int fail_membership_create;
  int fail_membership_close_once;
  int membership_close_failed;
  int connector_drain_failed;
  int route_enabled;
  int route_resolver_calls;
  int send_calls;
  int publish_calls;
  int claim_calls;
  int ack_calls;
  int requeue_calls;
  int live_components;
  flowie_cluster_peer_authorize_fn listener_authorize;
  void *listener_authorize_ctx;
  flowie_cluster_peer_authorize_fn connector_authorize;
  void *connector_authorize_ctx;
  flowie_cluster_node_edge_receive_fn edge_receive;
  void *edge_receive_ctx;
  char connect_remote_address[FLOWIE_CLUSTER_PEER_ADDRESS_MAX + 1u];
  char connect_transport_peer_address[FLOWIE_CLUSTER_PEER_ADDRESS_MAX + 1u];
  uint8_t connect_proxy_tlvs[32];
  size_t connect_proxy_tlvs_size;
} fake_cluster_state_t;

typedef struct cluster_fixture_s {
  flowie_cluster_node_config_t node;
  flowie_cluster_pgsql_config_t coordinator;
  flowie_cluster_membership_runtime_config_t membership;
  flowie_cluster_pgsql_fact_config_t fact;
  flowie_cluster_pgsql_fact_worker_config_t worker;
  flowie_cluster_session_bind_config_t session;
  flowie_cluster_shard_runtime_config_t shards[2];
  flowie_cluster_peer_connector_config_t connectors[2];
  flowie_cluster_topology_peer_t connector_peers[2];
} cluster_fixture_t;

static fake_cluster_state_t fake_cluster;

static void fake_cluster_reset(void) { memset(&fake_cluster, 0, sizeof(fake_cluster)); }

static void fake_cluster_event(const char *format, ...) {
  va_list args;
  int written;
  if (fake_cluster.event_size >= sizeof(fake_cluster.events)) return;
  va_start(args, format);
  written = vsnprintf(fake_cluster.events + fake_cluster.event_size,
                      sizeof(fake_cluster.events) - fake_cluster.event_size, format, args);
  va_end(args);
  if (written > 0 && (size_t)written < sizeof(fake_cluster.events) - fake_cluster.event_size)
    fake_cluster.event_size += (size_t)written;
}

static int fake_cluster_component_create(char kind, uint32_t id, void **out) {
  fake_cluster_component_t *component;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  component = (fake_cluster_component_t *)calloc(1u, sizeof(*component));
  if (!component) return TURBO_ENOMEM;
  component->kind = kind;
  component->id = id;
  ++fake_cluster.live_components;
  *out = component;
  return TURBO_OK;
}

static void fake_cluster_component_destroy(void *component) {
  if (!component) return;
  --fake_cluster.live_components;
  free(component);
}

static fake_cluster_component_t *fake_cluster_component(void *component, char kind) {
  fake_cluster_component_t *typed = (fake_cluster_component_t *)component;
  return typed && typed->kind == kind ? typed : NULL;
}

static int fake_router_create(const flowie_cluster_node_router_config_t *config,
                              flowie_cluster_node_router_t **out) {
  fake_cluster_event("R+;");
  if (!config || config->shard_count != 4u) return TURBO_EINVAL;
  return fake_cluster_component_create('R', 0u, (void **)out);
}

static int fake_router_register(flowie_cluster_node_router_t *router, uint32_t shard_id,
                                flowie_cluster_shard_runtime_t *runtime) {
  if (!fake_cluster_component(router, 'R') || !fake_cluster_component(runtime, 'S'))
    return TURBO_EINVAL;
  fake_cluster_event("G%u+;", shard_id);
  return TURBO_OK;
}

static int fake_router_unregister(flowie_cluster_node_router_t *router, uint32_t shard_id,
                                  flowie_cluster_shard_runtime_t *runtime) {
  if (!fake_cluster_component(router, 'R') || !fake_cluster_component(runtime, 'S'))
    return TURBO_EINVAL;
  fake_cluster_event("G%u-;", shard_id);
  return TURBO_OK;
}

static int fake_router_register_edge(flowie_cluster_node_router_t *router,
                                     flowie_cluster_node_edge_receive_fn receive,
                                     void *receive_ctx) {
  if (!fake_cluster_component(router, 'R') || !receive || !receive_ctx) return TURBO_EINVAL;
  if (fake_cluster.edge_receive) return TURBO_EBUSY;
  fake_cluster.edge_receive = receive;
  fake_cluster.edge_receive_ctx = receive_ctx;
  fake_cluster_event("E+;");
  return TURBO_OK;
}

static int fake_router_unregister_edge(flowie_cluster_node_router_t *router,
                                       flowie_cluster_node_edge_receive_fn receive,
                                       void *receive_ctx) {
  if (!fake_cluster_component(router, 'R') || !receive || !receive_ctx) return TURBO_EINVAL;
  if (fake_cluster.edge_receive != receive || fake_cluster.edge_receive_ctx != receive_ctx)
    return TURBO_EBUSY;
  fake_cluster.edge_receive = NULL;
  fake_cluster.edge_receive_ctx = NULL;
  fake_cluster_event("E-;");
  return TURBO_OK;
}

static tstr_t fake_cluster_connect_reply_payload(void) {
  static const uint8_t connack[] = {0x20u, 0x02u, 0x00u, 0x00u};
  const size_t total = FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE + sizeof(connack);
  tstr_t payload = tstr_new_len(NULL, total);
  uint8_t *bytes = (uint8_t *)payload;
  if (!payload) return NULL;
  memcpy(bytes, "TFBR", 4u);
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  flowie_cluster_peer_wire_write_u32(bytes + 12u,
                                     1u | ((uint32_t)FLOWIE_MQTT_VERSION_3_1_1 << 8u));
  flowie_cluster_peer_wire_write_u64(bytes + 16u, 9u);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, 17u);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, 3u);
  memcpy(bytes + FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE, connack, sizeof(connack));
  return payload;
}

static int fake_router_send(void *router, const flowie_cluster_peer_frame_t *frame,
                            flowie_cluster_peer_send_complete_fn complete, void *complete_ctx) {
  static const uint8_t puback[] = {0x40u, 0x02u, 0x00u, 0x2au};
  flowie_cluster_peer_frame_t reply = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  tstr_t payload = NULL;
  int rc = TURBO_OK;
  if (!fake_cluster_component(router, 'R') || !frame) return TURBO_EINVAL;
  ++fake_cluster.send_calls;
  if (complete) complete(complete_ctx, TURBO_OK);
  if (!fake_cluster.edge_receive || frame->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND)
    return TURBO_OK;
  if (frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND) {
    flowie_cluster_peer_connect_bind_view_t bind = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    rc = flowie_cluster_peer_connect_bind_decode(frame->payload.data, frame->payload.len,
                                                 frame->payload.len, &bind);
    if (rc != TURBO_OK || bind.proxy_tlvs.len > sizeof(fake_cluster.connect_proxy_tlvs))
      return rc == TURBO_OK ? TURBO_EMSGSIZE : rc;
    memcpy(fake_cluster.connect_remote_address, bind.remote_address.data,
           bind.remote_address.len);
    fake_cluster.connect_remote_address[bind.remote_address.len] = '\0';
    memcpy(fake_cluster.connect_transport_peer_address, bind.transport_peer_address.data,
           bind.transport_peer_address.len);
    fake_cluster.connect_transport_peer_address[bind.transport_peer_address.len] = '\0';
    if (bind.proxy_tlvs.len != 0u)
      memcpy(fake_cluster.connect_proxy_tlvs, bind.proxy_tlvs.data, bind.proxy_tlvs.len);
    fake_cluster.connect_proxy_tlvs_size = bind.proxy_tlvs.len;
    payload = fake_cluster_connect_reply_payload();
  } else if (frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH)
    rc = flowie_cluster_peer_mqtt_reply_encode(
        FLOWIE_MQTT_VERSION_3_1_1, (flowie_mqtt_span_t){puback, sizeof(puback)}, 0,
        TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 2048u, &payload);
  else if (frame->operation == FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST)
    rc = flowie_cluster_peer_mqtt_reply_encode(
        FLOWIE_MQTT_VERSION_3_1_1, (flowie_mqtt_span_t){NULL, 0u}, 0,
        (turbo_flow_protocol_settlement_point_t)0, 2048u, &payload);
  else
    return TURBO_OK;
  if (rc != TURBO_OK || !payload) return rc == TURBO_OK ? TURBO_ENOMEM : rc;
  reply.kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
  reply.shard_id = frame->shard_id;
  reply.status = TURBO_OK;
  reply.owner_epoch = frame->owner_epoch;
  reply.connection_id = frame->connection_id;
  reply.connection_generation = frame->connection_generation;
  reply.cluster_id = frame->cluster_id;
  reply.listener_id = frame->listener_id;
  reply.source_node_id = frame->target_node_id;
  reply.target_node_id = frame->source_node_id;
  reply.payload = tstr_to_v(payload);
  memcpy(reply.source_boot_id, frame->target_boot_id, sizeof(reply.source_boot_id));
  memcpy(reply.target_boot_id, frame->source_boot_id, sizeof(reply.target_boot_id));
  memcpy(reply.correlation_id, frame->correlation_id, sizeof(reply.correlation_id));
  rc = fake_cluster.edge_receive(fake_cluster.edge_receive_ctx, &reply);
  tstr_free(payload);
  return rc;
}

static int fake_router_close(flowie_cluster_node_router_t *router) {
  if (!fake_cluster_component(router, 'R')) return TURBO_EINVAL;
  fake_cluster_event("R<;");
  return TURBO_OK;
}

static int fake_router_drain(flowie_cluster_node_router_t *router, uint64_t timeout_ns) {
  if (!fake_cluster_component(router, 'R') || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("R~;");
  return TURBO_OK;
}

static int fake_router_destroy(flowie_cluster_node_router_t *router) {
  if (!fake_cluster_component(router, 'R')) return TURBO_EINVAL;
  fake_cluster_event("R-;");
  fake_cluster_component_destroy(router);
  return TURBO_OK;
}

static int fake_bus_create(const flowie_cluster_redis_bus_config_t *config,
                           flowie_cluster_redis_bus_t **out) {
  fake_cluster_event("B+;");
  if (!config || config->shard_count != 4u) return TURBO_EINVAL;
  fake_cluster.route_enabled = config->route_enabled;
  return fake_cluster_component_create('B', 0u, (void **)out);
}

static flowie_cluster_route_store_t *fake_route_store(flowie_cluster_redis_bus_t *bus) {
  return fake_cluster_component(bus, 'B') && fake_cluster.route_enabled
             ? (flowie_cluster_route_store_t *)(uintptr_t)1u
             : NULL;
}

static int fake_target_create(flowie_cluster_redis_bus_t *bus, uint32_t shard_id,
                              flowie_cluster_redis_target_t **out) {
  if (!fake_cluster_component(bus, 'B')) return TURBO_EINVAL;
  fake_cluster_event("T%u+;", shard_id);
  return fake_cluster_component_create('T', shard_id, (void **)out);
}

static int fake_publish(void *bus, const void *payload, size_t payload_size) {
  if (!fake_cluster_component(bus, 'B') || !payload || payload_size == 0u) return TURBO_EINVAL;
  ++fake_cluster.publish_calls;
  return TURBO_OK;
}

static int fake_target_claim(void *target, flowie_cluster_broadcast_target_claim_t *out) {
  fake_cluster_component_t *typed = fake_cluster_component(target, 'T');
  if (!typed || !out) return TURBO_EINVAL;
  ++fake_cluster.claim_calls;
  *out = (flowie_cluster_broadcast_target_claim_t)FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT;
  out->token = typed->id + 10u;
  out->payload = tstr_v_from_cstr("event");
  return TURBO_OK;
}

static int fake_target_ack(void *target, uint64_t token) {
  fake_cluster_component_t *typed = fake_cluster_component(target, 'T');
  if (!typed || token != typed->id + 10u) return TURBO_EINVAL;
  ++fake_cluster.ack_calls;
  return TURBO_OK;
}

static int fake_target_requeue(void *target, uint64_t token) {
  fake_cluster_component_t *typed = fake_cluster_component(target, 'T');
  if (!typed || token != typed->id + 10u) return TURBO_EINVAL;
  ++fake_cluster.requeue_calls;
  return TURBO_OK;
}

static void fake_target_destroy(flowie_cluster_redis_target_t *target) {
  fake_cluster_component_t *typed = fake_cluster_component(target, 'T');
  if (!typed) return;
  fake_cluster_event("T%u-;", typed->id);
  fake_cluster_component_destroy(target);
}

static int fake_bus_destroy(flowie_cluster_redis_bus_t *bus) {
  if (!fake_cluster_component(bus, 'B')) return TURBO_EINVAL;
  fake_cluster_event("B-;");
  fake_cluster_component_destroy(bus);
  return TURBO_OK;
}

static int fake_shard_create(const flowie_cluster_shard_runtime_config_t *config,
                             flowie_cluster_shard_runtime_t **out) {
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  flowie_cluster_broadcast_target_claim_t claim = FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT;
  static const char payload[] = "tfbe";
  int rc;
  fake_cluster_event("S%u+;", config ? config->shard_id : UINT32_MAX);
  if (!config || !config->reply || !config->broadcast_publish || !config->broadcast_target_claim ||
      !config->broadcast_target_ack || !config->broadcast_target_requeue ||
      !config->takeover_send || !config->delivery_send)
    return TURBO_EINVAL;
  if (fake_cluster.route_enabled) {
    flowie_cluster_pgsql_member_t member = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};
    if (!config->route_store || !config->route_member_resolve ||
        !config->route_member_resolve_ctx)
      return TURBO_EINVAL;
    ++fake_cluster.route_resolver_calls;
    if (config->route_member_resolve(config->route_member_resolve_ctx,
                                     tstr_v_from_cstr("node-a"), boot_id,
                                     &member) != TURBO_ENOENT)
      return TURBO_EPROTO;
  }
  if (config->shard_id == fake_cluster.fail_shard_id) return TURBO_EIO;
  rc = config->reply(config->reply_ctx, &frame);
  if (rc == TURBO_OK)
    rc = config->broadcast_publish(config->broadcast_publish_ctx, payload, sizeof(payload));
  if (rc == TURBO_OK)
    rc = config->broadcast_target_claim(config->broadcast_target_transport_ctx, &claim);
  if (rc == TURBO_OK)
    rc = config->broadcast_target_ack(config->broadcast_target_transport_ctx, claim.token);
  if (rc == TURBO_OK)
    rc = config->broadcast_target_requeue(config->broadcast_target_transport_ctx, claim.token);
  if (rc == TURBO_OK) rc = config->takeover_send(config->takeover_send_ctx, &frame, NULL, NULL);
  if (rc == TURBO_OK) rc = config->delivery_send(config->delivery_send_ctx, &frame, NULL, NULL);
  if (rc != TURBO_OK) return rc;
  return fake_cluster_component_create('S', config->shard_id, (void **)out);
}

static int fake_shard_close(flowie_cluster_shard_runtime_t *runtime, uint64_t timeout_ns) {
  fake_cluster_component_t *typed = fake_cluster_component(runtime, 'S');
  if (!typed || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("S%u<;", typed->id);
  return TURBO_OK;
}

static int fake_shard_destroy(flowie_cluster_shard_runtime_t *runtime) {
  fake_cluster_component_t *typed = fake_cluster_component(runtime, 'S');
  if (!typed) return TURBO_EINVAL;
  fake_cluster_event("S%u-;", typed->id);
  fake_cluster_component_destroy(runtime);
  return TURBO_OK;
}

static int fake_listener_create(const flowie_cluster_peer_listener_config_t *config,
                                flowie_cluster_peer_listener_t **out) {
  fake_cluster_event("L+;");
  if (!config || !fake_cluster_component(config->router, 'R')) return TURBO_EINVAL;
  fake_cluster.listener_authorize = config->authorize;
  fake_cluster.listener_authorize_ctx = config->authorize_ctx;
  return fake_cluster_component_create('L', 0u, (void **)out);
}

static int fake_listener_start(flowie_cluster_peer_listener_t *listener, uint64_t timeout_ns) {
  if (!fake_cluster_component(listener, 'L') || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("L>;");
  return TURBO_OK;
}

static int fake_listener_close(flowie_cluster_peer_listener_t *listener, uint64_t timeout_ns) {
  if (!fake_cluster_component(listener, 'L') || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("L<;");
  return TURBO_OK;
}

static int fake_listener_drain(flowie_cluster_peer_listener_t *listener, uint64_t timeout_ns) {
  if (!fake_cluster_component(listener, 'L') || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("L~;");
  return TURBO_OK;
}

static int fake_listener_destroy(flowie_cluster_peer_listener_t *listener) {
  if (!fake_cluster_component(listener, 'L')) return TURBO_EINVAL;
  fake_cluster_event("L-;");
  fake_cluster_component_destroy(listener);
  return TURBO_OK;
}

static uint32_t fake_connector_id(const flowie_cluster_peer_connector_config_t *config) {
  return config && config->remote_node_id.len != 0u
             ? (uint32_t)(uint8_t)config->remote_node_id.data[config->remote_node_id.len - 1u]
             : 0u;
}

static int fake_connector_create(const flowie_cluster_peer_connector_config_t *config,
                                 flowie_cluster_peer_connector_t **out) {
  uint32_t id = fake_connector_id(config);
  fake_cluster_event("C%c+;", (char)id);
  if (!config || !fake_cluster_component(config->router, 'R')) return TURBO_EINVAL;
  fake_cluster.connector_authorize = config->authorize;
  fake_cluster.connector_authorize_ctx = config->authorize_ctx;
  return fake_cluster_component_create('C', id, (void **)out);
}

static int fake_connector_start(flowie_cluster_peer_connector_t *connector, uint64_t timeout_ns) {
  fake_cluster_component_t *typed = fake_cluster_component(connector, 'C');
  if (!typed || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("C%c>;", (char)typed->id);
  return typed->id == fake_cluster.fail_connector_start_id ? TURBO_EIO : TURBO_OK;
}

static int fake_connector_close(flowie_cluster_peer_connector_t *connector, uint64_t timeout_ns) {
  fake_cluster_component_t *typed = fake_cluster_component(connector, 'C');
  if (!typed || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("C%c<;", (char)typed->id);
  return TURBO_OK;
}

static int fake_connector_drain(flowie_cluster_peer_connector_t *connector, uint64_t timeout_ns) {
  fake_cluster_component_t *typed = fake_cluster_component(connector, 'C');
  if (!typed || timeout_ns == 0u) return TURBO_EINVAL;
  fake_cluster_event("C%c~;", (char)typed->id);
  if (typed->id == fake_cluster.fail_connector_drain_once_id &&
      !fake_cluster.connector_drain_failed) {
    fake_cluster.connector_drain_failed = 1;
    return TURBO_EBUSY;
  }
  return TURBO_OK;
}

static int fake_connector_destroy(flowie_cluster_peer_connector_t *connector) {
  fake_cluster_component_t *typed = fake_cluster_component(connector, 'C');
  if (!typed) return TURBO_EINVAL;
  fake_cluster_event("C%c-;", (char)typed->id);
  fake_cluster_component_destroy(connector);
  return TURBO_OK;
}

static int fake_membership_create(const flowie_cluster_membership_runtime_config_t *config,
                                  flowie_cluster_membership_runtime_t **out) {
  fake_cluster_event("M+;");
  if (!config || !config->current || !config->apply || !config->topology_ctx) return TURBO_EINVAL;
  if (fake_cluster.route_enabled &&
      (!config->member_directory || !config->maintenance || !config->maintenance_ctx))
    return TURBO_EINVAL;
  if (fake_cluster.fail_membership_create) return TURBO_EIO;
  return fake_cluster_component_create('M', 0u, (void **)out);
}

static int fake_membership_start(flowie_cluster_membership_runtime_t *runtime) {
  if (!fake_cluster_component(runtime, 'M')) return TURBO_EINVAL;
  fake_cluster_event("M>;");
  return TURBO_OK;
}

static int fake_membership_close(flowie_cluster_membership_runtime_t *runtime,
                                 uint64_t timeout_ns) {
  if (!fake_cluster_component(runtime, 'M')) return TURBO_EINVAL;
  fake_cluster_event("M<;");
  if (timeout_ns == 0u) return TURBO_EINVAL;
  if (fake_cluster.fail_membership_close_once && !fake_cluster.membership_close_failed) {
    fake_cluster.membership_close_failed = 1;
    return TURBO_ETIMEDOUT;
  }
  return TURBO_OK;
}

static void fake_membership_destroy(flowie_cluster_membership_runtime_t *runtime) {
  if (!fake_cluster_component(runtime, 'M')) return;
  fake_cluster_event("M-;");
  fake_cluster_component_destroy(runtime);
}

static const flowie_cluster_node_api_t FAKE_CLUSTER_API = {
    sizeof(flowie_cluster_node_api_t),
    FLOWIE_CLUSTER_NODE_ABI_V1,
    {fake_router_create, fake_router_register, fake_router_unregister, fake_router_register_edge,
     fake_router_unregister_edge, fake_router_send, fake_router_close, fake_router_drain,
     fake_router_destroy},
    {fake_bus_create, fake_target_create, fake_publish, fake_target_claim, fake_target_ack,
     fake_target_requeue, fake_target_destroy, fake_bus_destroy, fake_route_store},
    {fake_shard_create, fake_shard_close, fake_shard_destroy},
    {fake_listener_create, fake_listener_start, fake_listener_close, fake_listener_drain,
     fake_listener_destroy},
    {fake_connector_create, fake_connector_start, fake_connector_close, fake_connector_drain,
     fake_connector_destroy},
    {fake_membership_create, fake_membership_start, fake_membership_close,
     fake_membership_destroy}};

static int cluster_fixture_configure_connector(void *ctx,
                                               const flowie_cluster_topology_peer_t *peer,
                                               flowie_cluster_peer_connector_config_t *out) {
  cluster_fixture_t *fixture = (cluster_fixture_t *)ctx;
  if (!fixture || !peer || !out) return TURBO_EINVAL;
  *out = (flowie_cluster_peer_connector_config_t)FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT;
  out->execution = (struct tf_coronet_execution_s *)(uintptr_t)1u;
  out->remote_host = peer->advertised_endpoint;
  out->remote_port = 7100u;
  out->cluster_id = fixture->node.router.cluster_id;
  out->local_node_id = fixture->node.router.local_node_id;
  memcpy(out->local_boot_id, fixture->node.router.local_boot_id, sizeof(out->local_boot_id));
  out->remote_node_id = peer->node_id;
  memcpy(out->remote_boot_id, peer->boot_id, sizeof(out->remote_boot_id));
  return TURBO_OK;
}

static void cluster_fixture_init(cluster_fixture_t *fixture) {
  static const uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u, 2u,  3u,  4u,  5u,  6u,  7u,  8u,
                                                            9u, 10u, 11u, 12u, 13u, 14u, 15u, 16u};
  memset(fixture, 0, sizeof(*fixture));
  fixture->node = (flowie_cluster_node_config_t)FLOWIE_CLUSTER_NODE_CONFIG_INIT;
  fixture->coordinator = (flowie_cluster_pgsql_config_t)FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  fixture->membership =
      (flowie_cluster_membership_runtime_config_t)FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_CONFIG_INIT;
  fixture->fact = (flowie_cluster_pgsql_fact_config_t)FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  fixture->worker =
      (flowie_cluster_pgsql_fact_worker_config_t)FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
  fixture->session = (flowie_cluster_session_bind_config_t)FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  fixture->coordinator.cluster_id = "alpha";
  fixture->coordinator.conninfo = "host=127.0.0.1 port=1 connect_timeout=1";
  fixture->coordinator.schema_name = "flowie_cluster";
  fixture->coordinator.listener_id = "mqtt";
  fixture->coordinator.node_id = "node-a";
  fixture->coordinator.advertised_endpoint = "127.0.0.1:7101";
  fixture->coordinator.shard_count = 4u;
  fixture->coordinator.lease_ttl_ms = 10000u;
  fixture->coordinator.renew_interval_ms = 1000u;
  fixture->coordinator.retry_interval_ms = 10u;
  fixture->coordinator.worst_case_db_latency_ms = 100u;
  fixture->coordinator.safety_margin_ms = 100u;
  memcpy(fixture->coordinator.boot_id, boot, sizeof(boot));
  fixture->fact.coordinator = &fixture->coordinator;
  fixture->worker.fact = &fixture->fact;

  fixture->node.router.shard_count = 4u;
  fixture->node.router.max_links = 2u;
  fixture->node.router.max_inflight_sends = 8u;
  fixture->node.router.cluster_id = tstr_v_from_cstr("alpha");
  fixture->node.router.listener_id = tstr_v_from_cstr("mqtt");
  fixture->node.router.local_node_id = tstr_v_from_cstr("node-a");
  memcpy(fixture->node.router.local_boot_id, boot, sizeof(boot));
  fixture->node.redis.cluster_id = "alpha";
  fixture->node.redis.listener_id = "mqtt";
  fixture->node.redis.shard_count = 4u;
  fixture->node.listener.execution = (struct tf_coronet_execution_s *)(uintptr_t)1u;
  fixture->node.listener.cluster_id = tstr_v_from_cstr("alpha");
  fixture->node.listener.local_node_id = tstr_v_from_cstr("node-a");
  memcpy(fixture->node.listener.local_boot_id, boot, sizeof(boot));

  for (size_t index = 0u; index < 2u; ++index) {
    flowie_cluster_shard_runtime_config_t *shard = &fixture->shards[index];
    *shard = (flowie_cluster_shard_runtime_config_t)FLOWIE_CLUSTER_SHARD_RUNTIME_CONFIG_INIT;
    shard->shard_id = (uint32_t)(2u - index);
    shard->execution = (struct tf_coronet_execution_s *)(uintptr_t)1u;
    shard->fact_worker = &fixture->worker;
    shard->session_bind = &fixture->session;
    shard->broadcast_max_payload_size = 1024u;
    shard->broadcast_poll_interval_ns = 1u;
    shard->broadcast_ack_poll_interval_ns = 1u;
    shard->broadcast_retry_interval_ns = 1u;
    shard->broadcast_republish_interval_ns = 1u;
    shard->broadcast_target_max_payload_size = 1024u;
    shard->broadcast_target_poll_interval_ns = 1u;
    shard->broadcast_target_retry_interval_ns = 1u;
    shard->broadcast_target_ack_retry_interval_ns = 1u;
    shard->takeover_poll_interval_ns = 1u;
    shard->takeover_retry_interval_ns = 1u;
    shard->takeover_reply_timeout_ns = 1u;
    shard->delivery_poll_interval_ns = 1u;
    shard->delivery_retry_interval_ns = 1u;
    shard->delivery_reply_timeout_ns = 1u;
  }
  fixture->node.local_shards = fixture->shards;
  fixture->node.local_shard_count = 2u;

  for (size_t index = 0u; index < 2u; ++index) {
    flowie_cluster_peer_connector_config_t *connector = &fixture->connectors[index];
    *connector = (flowie_cluster_peer_connector_config_t)FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT;
    connector->execution = (struct tf_coronet_execution_s *)(uintptr_t)1u;
    connector->cluster_id = tstr_v_from_cstr("alpha");
    connector->local_node_id = tstr_v_from_cstr("node-a");
    connector->remote_node_id = tstr_v_from_cstr(index == 0u ? "node-c" : "node-b");
    memcpy(connector->local_boot_id, boot, sizeof(boot));
    connector->remote_boot_id[0] = (uint8_t)(index + 2u);
    fixture->connector_peers[index] =
        (flowie_cluster_topology_peer_t)FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT;
    fixture->connector_peers[index].node_id = connector->remote_node_id;
    memcpy(fixture->connector_peers[index].boot_id, connector->remote_boot_id,
           sizeof(fixture->connector_peers[index].boot_id));
    fixture->connector_peers[index].advertised_endpoint =
        tstr_v_from_cstr(index == 0u ? "127.0.0.1:7103" : "127.0.0.1:7102");
  }
  fixture->node.connectors = fixture->connectors;
  fixture->node.connector_peers = fixture->connector_peers;
  fixture->node.connector_count = 2u;
  fixture->node.topology_revision = 1u;
  fixture->node.connector_configure = cluster_fixture_configure_connector;
  fixture->node.connector_configure_ctx = fixture;
  fixture->membership.coordinator = &fixture->coordinator;
  fixture->membership.topology.local_node_id = fixture->node.router.local_node_id;
  memcpy(fixture->membership.topology.local_boot_id, boot, sizeof(boot));
  fixture->membership.topology.max_nodes = 3u;
  fixture->membership.topology.max_endpoint_size = FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX;
  fixture->membership.refresh_interval_ns = 1000000u;
  fixture->membership.retry_interval_ns = 1000000u;
  fixture->membership.apply_timeout_ns = 1000000u;
}

static flowie_cluster_topology_member_t
cluster_topology_member(const char *node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                        flowie_cluster_node_state_t state, const char *endpoint,
                        uint64_t revision) {
  flowie_cluster_topology_member_t member = FLOWIE_CLUSTER_TOPOLOGY_MEMBER_INIT;
  member.node_id = tstr_v_from_cstr(node_id);
  memcpy(member.boot_id, boot_id, sizeof(member.boot_id));
  member.state = state;
  member.advertised_endpoint = tstr_v_from_cstr(endpoint);
  member.revision = revision;
  return member;
}

static void cluster_certificate_fingerprint(
    char output[FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE], char digit) {
  memcpy(output, "sha256:", 7u);
  memset(output + 7u, digit, 64u);
  output[FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE - 1u] = '\0';
}

static int cluster_test_edge_receive(void *ctx, const flowie_cluster_peer_frame_t *frame) {
  return ctx && frame ? TURBO_OK : TURBO_EINVAL;
}

typedef struct cluster_endpoint_test_state_s {
  atomic_size_t completions;
  int last_status;
  uint8_t last_packet_type;
  turbo_flow_protocol_settlement_point_t last_settlement;
  int socket_actions;
  int takeover_closes;
} cluster_endpoint_test_state_t;

typedef struct cluster_endpoint_connect_call_s {
  const flowie_endpoint_cluster_binding_t *port;
  const flowie_mqtt_connect_view_t *connect;
  const turbo_flow_security_principal_t *principal;
  const flowie_endpoint_cluster_socket_port_t *socket_port;
  cluster_endpoint_test_state_t *state;
} cluster_endpoint_connect_call_t;

typedef struct cluster_endpoint_command_call_s {
  const flowie_endpoint_cluster_binding_t *port;
  flowie_mqtt_span_t packet;
  cluster_endpoint_test_state_t *state;
} cluster_endpoint_command_call_t;

static void cluster_endpoint_complete(void *ctx, int status,
                                      const flowie_endpoint_cluster_action_t *action) {
  cluster_endpoint_test_state_t *state = (cluster_endpoint_test_state_t *)ctx;
  if (!state) return;
  state->last_status = status;
  state->last_packet_type =
      status == TURBO_OK && action && action->packet.data ? action->packet.data[0] : 0u;
  state->last_settlement =
      status == TURBO_OK && action ? action->settlement_point
                                   : (turbo_flow_protocol_settlement_point_t)0;
  atomic_fetch_add_explicit(&state->completions, 1u, memory_order_release);
}

static int cluster_endpoint_socket_takeover(void *ctx) {
  cluster_endpoint_test_state_t *state = (cluster_endpoint_test_state_t *)ctx;
  if (!state) return TURBO_EINVAL;
  ++state->takeover_closes;
  return TURBO_OK;
}

static int cluster_endpoint_socket_action(void *ctx,
                                          const flowie_endpoint_cluster_action_t *action) {
  cluster_endpoint_test_state_t *state = (cluster_endpoint_test_state_t *)ctx;
  if (!state || !action) return TURBO_EINVAL;
  ++state->socket_actions;
  return TURBO_OK;
}

static int cluster_endpoint_connect_call(void *arg) {
  static const uint8_t proxy_tlvs[] = {0xe0u, 0x00u, 0x02u, 'a', 'b'};
  cluster_endpoint_connect_call_t *call = (cluster_endpoint_connect_call_t *)arg;
  flowie_endpoint_cluster_ingress_t ingress = FLOWIE_ENDPOINT_CLUSTER_INGRESS_INIT;
  ingress.remote_address = "203.0.113.9:45678";
  ingress.transport_peer_address = "127.0.0.1:443";
  ingress.proxy_tlvs = (flowie_mqtt_span_t){proxy_tlvs, sizeof(proxy_tlvs)};
  return call->port->connect(call->port->ctx, 41u, 3u, call->connect, call->principal,
                             &ingress, call->socket_port, cluster_endpoint_complete, call->state);
}

static int cluster_endpoint_command_call(void *arg) {
  static const uint8_t client_id[] = "client-a";
  cluster_endpoint_command_call_t *call = (cluster_endpoint_command_call_t *)arg;
  return call->port->command(
      call->port->ctx, 41u, 3u, FLOWIE_ENDPOINT_CLUSTER_COMMAND_PUBLISH,
      FLOWIE_MQTT_VERSION_3_1_1,
      (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, call->packet,
      cluster_endpoint_complete, call->state);
}

static int cluster_endpoint_lost_detach_call(void *arg) {
  static const uint8_t client_id[] = "client-a";
  const flowie_endpoint_cluster_binding_t *port =
      (const flowie_endpoint_cluster_binding_t *)arg;
  flowie_mqtt_span_t id = {client_id, sizeof(client_id) - 1u};
  int rc = port->connection_lost(port->ctx, 41u, 3u, FLOWIE_MQTT_VERSION_3_1_1, id);
  port->detach(port->ctx, 41u, 3u);
  return rc;
}

static int cluster_endpoint_connect_view(flowie_mqtt_packet_view_t *packet,
                                         flowie_mqtt_connect_view_t *connect, uint8_t *wire,
                                         size_t capacity) {
  static const uint8_t client_id[] = "client-a";
  flowie_mqtt_connect_packet_t description = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  size_t written = 0u;
  size_t consumed = 0u;
  int rc;
  description.version = FLOWIE_MQTT_VERSION_3_1_1;
  description.clean_start = 1u;
  description.keep_alive = 30u;
  description.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  rc = flowie_mqtt_connect_packet_encode(&description, wire, capacity, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  options.max_packet_size = capacity;
  rc = flowie_mqtt_packet_parse(wire, written, &options, packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != written) return TURBO_EPROTO;
  return flowie_mqtt_connect_parse(packet, connect) == FLOWIE_MQTT_PARSE_OK ? TURBO_OK
                                                                           : TURBO_EPROTO;
}

spec("Flowie cluster node composition") {
  before_each() { fake_cluster_reset(); }

  it("rejects route projection without membership before opening infrastructure") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.node.redis.route_enabled = 1;
    fixture.shards[0].route_poll_interval_ns = 1u;
    fixture.shards[0].route_retry_interval_ns = 1u;
    fixture.shards[1].route_poll_interval_ns = 1u;
    fixture.shards[1].route_retry_interval_ns = 1u;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_EINVAL);
    check_null(node);
    check_str_eq(fake_cluster.events, "");
  }

  it("injects one empty member directory into every route-enabled shard") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.node.redis.route_enabled = 1;
    fixture.node.membership = &fixture.membership;
    fixture.shards[0].route_poll_interval_ns = 1u;
    fixture.shards[0].route_retry_interval_ns = 1u;
    fixture.shards[1].route_poll_interval_ns = 1u;
    fixture.shards[1].route_retry_interval_ns = 1u;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_not_null(node);
    check_int_eq(fake_cluster.route_resolver_calls, 2);
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("rejects route projection when any local shard lacks its polling cadence") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.node.redis.route_enabled = 1;
    fixture.node.membership = &fixture.membership;
    fixture.shards[0].route_poll_interval_ns = 1u;
    fixture.shards[0].route_retry_interval_ns = 1u;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_EINVAL);
    check_null(node);
    check_str_eq(fake_cluster.events, "");
  }

  it("rejects duplicate local ownership before opening infrastructure") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.shards[1].shard_id = fixture.shards[0].shard_id;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_EINVAL);
    check_null(node);
    check_str_eq(fake_cluster.events, "");
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("exposes one borrowed edge port and the node-wide command submit path") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    int sends_before;
    cluster_fixture_init(&fixture);
    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    sends_before = fake_cluster.send_calls;
    check_int_eq(flowie_cluster_node_register_edge(node, cluster_test_edge_receive, &fixture),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_submit(node, &frame), TURBO_OK);
    check_int_eq(fake_cluster.send_calls, sends_before + 1);
    check_int_eq(flowie_cluster_node_unregister_edge(node, cluster_test_edge_receive, &fixture),
                 TURBO_OK);
    check_str_contains(fake_cluster.events, "E+;E-;");
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("adapts endpoint requests through the owner directory and exact node route") {
    static const uint8_t publish[] = {0x32u, 0x06u, 0x00u, 0x01u,
                                      'a',   0x00u, 0x2au, 0x00u};
    static const uint8_t proxy_tlvs[] = {0xe0u, 0x00u, 0x02u, 'a', 'b'};
    turbo_flow_coronet_execution_binding_t placement = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    tf_coronet_execution_t execution;
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    flowie_cluster_owner_directory_config_t directory_config =
        FLOWIE_CLUSTER_OWNER_DIRECTORY_CONFIG_INIT;
    flowie_cluster_owner_directory_entry_t entries[4];
    flowie_cluster_owner_directory_t *directory = NULL;
    flowie_cluster_endpoint_binding_config_t endpoint_config =
        FLOWIE_CLUSTER_ENDPOINT_BINDING_CONFIG_INIT;
    flowie_cluster_endpoint_binding_t *endpoint_binding = NULL;
    const flowie_endpoint_cluster_binding_t *port;
    flowie_endpoint_cluster_socket_port_t socket_port =
        FLOWIE_ENDPOINT_CLUSTER_SOCKET_PORT_INIT;
    cluster_endpoint_test_state_t state;
    cluster_endpoint_connect_call_t connect_call;
    cluster_endpoint_command_call_t command_call;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    uint8_t wire[128];
    int close_rc = TURBO_EBUSY;

    memset(&execution, 0, sizeof(execution));
    memset(&state, 0, sizeof(state));
    atomic_init(&state.completions, 0u);
    cluster_fixture_init(&fixture);
    check_int_eq(tf_coronet_execution_init(&execution, &placement), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    directory_config.shard_count = 4u;
    directory_config.cluster_id = fixture.node.router.cluster_id;
    directory_config.listener_id = fixture.node.router.listener_id;
    check_int_eq(flowie_cluster_owner_directory_create(&directory_config, &directory), TURBO_OK);
    for (uint32_t shard = 0u; shard < 4u; ++shard) {
      entries[shard] =
          (flowie_cluster_owner_directory_entry_t)FLOWIE_CLUSTER_OWNER_DIRECTORY_ENTRY_INIT;
      entries[shard].shard_id = shard;
      entries[shard].local_deadline_ns = turbo_hrtime() + UINT64_C(5000000000);
      entries[shard].owner.shard_id = shard;
      entries[shard].owner.owner_epoch = 7u;
      entries[shard].owner.node_id_size = fixture.node.router.local_node_id.len;
      memcpy(entries[shard].owner.node_id, fixture.node.router.local_node_id.data,
             fixture.node.router.local_node_id.len);
      memcpy(entries[shard].owner.boot_id, fixture.node.router.local_boot_id,
             sizeof(entries[shard].owner.boot_id));
    }
    check_int_eq(flowie_cluster_owner_directory_replace(directory, entries, 4u, 1u), TURBO_OK);
    endpoint_config.execution = &execution;
    endpoint_config.node = node;
    endpoint_config.owners = directory;
    endpoint_config.max_payload_size = 2048u;
    endpoint_config.max_connections = 4u;
    endpoint_config.max_connection_bytes = 4096u;
    endpoint_config.max_pending_bytes = 16384u;
    endpoint_config.max_inbound_actions = 4u;
    endpoint_config.max_inbound_bytes = 8192u;
    endpoint_config.cluster_id = fixture.node.router.cluster_id;
    endpoint_config.listener_id = fixture.node.router.listener_id;
    endpoint_config.local_node_id = fixture.node.router.local_node_id;
    memcpy(endpoint_config.local_boot_id, fixture.node.router.local_boot_id,
           sizeof(endpoint_config.local_boot_id));
    check_int_eq(flowie_cluster_endpoint_binding_create(&endpoint_config, &endpoint_binding),
                 TURBO_OK);
    port = flowie_cluster_endpoint_binding_port(endpoint_binding);
    check_not_null(port);
    socket_port.ctx = &state;
    socket_port.takeover_close = cluster_endpoint_socket_takeover;
    socket_port.apply_action = cluster_endpoint_socket_action;
    check_int_eq(cluster_endpoint_connect_view(&packet, &connect, wire, sizeof(wire)), TURBO_OK);
    memset(&connect_call, 0, sizeof(connect_call));
    connect_call.port = port;
    connect_call.connect = &connect;
    connect_call.principal = NULL;
    connect_call.socket_port = &socket_port;
    connect_call.state = &state;
    check_int_eq(tf_coronet_execution_call(&execution, cluster_endpoint_connect_call,
                                           &connect_call, UINT64_C(5000000000)),
                 TURBO_OK);
    for (size_t i = 0u; i < 2000u &&
                        atomic_load_explicit(&state.completions, memory_order_acquire) < 1u;
         ++i)
      turbo_sleep_ms(1u);
    check_uint_eq(atomic_load_explicit(&state.completions, memory_order_acquire), 1u);
    check_int_eq(state.last_status, TURBO_OK);
    check_uint_eq(state.last_packet_type, 0x20u);
    check_str_eq(fake_cluster.connect_remote_address, "203.0.113.9:45678");
    check_str_eq(fake_cluster.connect_transport_peer_address, "127.0.0.1:443");
    check_size_eq(fake_cluster.connect_proxy_tlvs_size, sizeof(proxy_tlvs));
    check_mem_eq(fake_cluster.connect_proxy_tlvs, proxy_tlvs, sizeof(proxy_tlvs));
    command_call.port = port;
    command_call.packet = (flowie_mqtt_span_t){publish, sizeof(publish)};
    command_call.state = &state;
    check_int_eq(tf_coronet_execution_call(&execution, cluster_endpoint_command_call,
                                           &command_call, UINT64_C(5000000000)),
                 TURBO_OK);
    for (size_t i = 0u; i < 2000u &&
                        atomic_load_explicit(&state.completions, memory_order_acquire) < 2u;
         ++i)
      turbo_sleep_ms(1u);
    check_uint_eq(atomic_load_explicit(&state.completions, memory_order_acquire), 2u);
    check_int_eq(state.last_status, TURBO_OK);
    check_uint_eq(state.last_packet_type, 0x40u);
    check_uint_eq(state.last_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(tf_coronet_execution_call(&execution, cluster_endpoint_lost_detach_call,
                                           (void *)port, UINT64_C(5000000000)),
                 TURBO_OK);
    for (size_t i = 0u; i < 2000u && close_rc == TURBO_EBUSY; ++i) {
      close_rc = flowie_cluster_endpoint_binding_close(endpoint_binding,
                                                       UINT64_C(5000000000));
      if (close_rc == TURBO_EBUSY) turbo_sleep_ms(1u);
    }
    check_int_eq(close_rc, TURBO_OK);
    check_int_eq(flowie_cluster_endpoint_binding_close(endpoint_binding,
                                                       UINT64_C(5000000000)),
                 TURBO_OK);
    check_int_eq(flowie_cluster_endpoint_binding_destroy(endpoint_binding), TURBO_OK);
    flowie_cluster_owner_directory_destroy(directory);
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("wires transports and enforces the ownership-safe lifecycle order") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.node.membership = &fixture.membership;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_not_null(node);
    check_str_eq(fake_cluster.events, "R+;B+;T1+;S1+;G1+;T2+;S2+;G2+;L+;Cb+;Cc+;M+;");
    check_int_eq(fake_cluster.send_calls, 6);
    check_int_eq(fake_cluster.publish_calls, 2);
    check_int_eq(fake_cluster.claim_calls, 2);
    check_int_eq(fake_cluster.ack_calls, 2);
    check_int_eq(fake_cluster.requeue_calls, 2);

    check_int_eq(flowie_cluster_node_start(node, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_node_close(node, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_str_eq(fake_cluster.events,
                 "R+;B+;T1+;S1+;G1+;T2+;S2+;G2+;L+;Cb+;Cc+;M+;L>;Cb>;Cc>;M>;M<;"
                 "Cc<;Cb<;L<;Cc~;Cb~;L~;R<;R~;G1-;S1<;G2-;S2<;M-;Cc-;Cb-;L-;"
                 "S2-;T2-;S1-;T1-;R-;B-;");
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("wires one membership and certificate authority into every peer transport") {
    cluster_fixture_t fixture;
    flowie_cluster_peer_certificate_pin_t pins[2] = {
        FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT, FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT};
    flowie_cluster_peer_authority_config_t authority = FLOWIE_CLUSTER_PEER_AUTHORITY_CONFIG_INIT;
    char fingerprints[2][FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE];
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    cluster_certificate_fingerprint(fingerprints[0], 'b');
    cluster_certificate_fingerprint(fingerprints[1], 'c');
    pins[0].node_id = tstr_v_from_cstr("node-b");
    pins[0].certificate_sha256 = fingerprints[0];
    pins[1].node_id = tstr_v_from_cstr("node-c");
    pins[1].certificate_sha256 = fingerprints[1];
    authority.max_peers = 2u;
    authority.pins = pins;
    authority.pin_count = 2u;
    fixture.node.peer_authority = &authority;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_not_null(node);
    check_not_null(fake_cluster.listener_authorize);
    check_not_null(fake_cluster.connector_authorize);
    check_int_eq(fake_cluster.listener_authorize == fake_cluster.connector_authorize, 1);
    check_int_eq(fake_cluster.listener_authorize_ctx == fake_cluster.connector_authorize_ctx, 1);
    check_int_eq(fake_cluster.listener_authorize(
                     fake_cluster.listener_authorize_ctx, tstr_v_from_cstr("node-b"),
                     fixture.connector_peers[1].boot_id, fingerprints[0]),
                 TURBO_OK);
    check_int_eq(fake_cluster.listener_authorize(
                     fake_cluster.listener_authorize_ctx, tstr_v_from_cstr("node-b"),
                     fixture.connector_peers[1].boot_id, fingerprints[1]),
                 TURBO_EPERM);
    fixture.connector_peers[1].boot_id[1] = 1u;
    check_int_eq(fake_cluster.listener_authorize(
                     fake_cluster.listener_authorize_ctx, tstr_v_from_cstr("node-b"),
                     fixture.connector_peers[1].boot_id, fingerprints[0]),
                 TURBO_EPERM);
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("rolls back every earlier component when a later shard fails") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fake_cluster.fail_shard_id = 2u;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_EIO);
    check_null(node);
    check_int_eq(fake_cluster.live_components, 0);
    check_str_eq(fake_cluster.events, "R+;B+;T1+;S1+;G1+;T2+;S2+;R<;R~;G1-;S1<;T2-;S1-;T1-;R-;B-;");
  }

  it("rolls back the complete generation when membership creation fails") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.node.membership = &fixture.membership;
    fake_cluster.fail_membership_create = 1;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_EIO);
    check_null(node);
    check_not_null(strstr(fake_cluster.events, "M+;Cc<;Cb<;L<;"));
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("keeps close retryable until every peer has drained") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fake_cluster.fail_connector_drain_once_id = (uint32_t)'c';

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_start(node, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_node_close(node, UINT64_MAX), TURBO_EBUSY);
    check_null(strstr(fake_cluster.events, "R<;"));
    check_int_eq(flowie_cluster_node_close(node, UINT64_MAX), TURBO_OK);
    check_not_null(strstr(fake_cluster.events, "Cc~;R<;R~;"));
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("does not tear down peers until the membership worker has joined") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fixture.node.membership = &fixture.membership;
    fake_cluster.fail_membership_close_once = 1;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_start(node, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_node_close(node, UINT64_C(1000000)), TURBO_ETIMEDOUT);
    check_null(strstr(fake_cluster.events, "Cc<;"));
    check_null(strstr(fake_cluster.events, "L<;"));
    check_int_eq(flowie_cluster_node_close(node, UINT64_MAX), TURBO_OK);
    check_not_null(strstr(fake_cluster.events, "M<;M<;Cc<;"));
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("applies a topology revision atomically after retrying a partial replacement") {
    cluster_fixture_t fixture;
    flowie_cluster_peer_certificate_pin_t pins[3] = {
        FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT, FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT,
        FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT};
    flowie_cluster_peer_authority_config_t authority = FLOWIE_CLUSTER_PEER_AUTHORITY_CONFIG_INIT;
    char fingerprints[3][FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE];
    flowie_cluster_topology_peer_t current[2];
    flowie_cluster_topology_member_t members[4];
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_topology_plan_config_t plan_config = FLOWIE_CLUSTER_TOPOLOGY_PLAN_CONFIG_INIT;
    flowie_cluster_topology_plan_t *plan = NULL;
    flowie_cluster_node_t *node = NULL;
    uint8_t boot_b[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {9u};
    uint8_t boot_d[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {4u};
    size_t current_count = 0u;
    uint64_t revision = 0u;
    cluster_fixture_init(&fixture);
    cluster_certificate_fingerprint(fingerprints[0], 'b');
    cluster_certificate_fingerprint(fingerprints[1], 'c');
    cluster_certificate_fingerprint(fingerprints[2], 'd');
    pins[0].node_id = tstr_v_from_cstr("node-b");
    pins[0].certificate_sha256 = fingerprints[0];
    pins[1].node_id = tstr_v_from_cstr("node-c");
    pins[1].certificate_sha256 = fingerprints[1];
    pins[2].node_id = tstr_v_from_cstr("node-d");
    pins[2].certificate_sha256 = fingerprints[2];
    authority.max_peers = 2u;
    authority.pins = pins;
    authority.pin_count = 3u;
    fixture.node.peer_authority = &authority;
    members[0] = cluster_topology_member("node-a", fixture.node.router.local_boot_id,
                                         FLOWIE_CLUSTER_NODE_READY, "127.0.0.1:7101", 2u);
    members[1] =
        cluster_topology_member("node-b", boot_b, FLOWIE_CLUSTER_NODE_READY, "127.0.0.1:7202", 2u);
    members[2] = cluster_topology_member("node-c", fixture.connector_peers[0].boot_id,
                                         FLOWIE_CLUSTER_NODE_OFFLINE, "127.0.0.1:7103", 2u);
    members[3] =
        cluster_topology_member("node-d", boot_d, FLOWIE_CLUSTER_NODE_READY, "127.0.0.1:7104", 2u);
    membership.membership_revision = 2u;
    membership.members = members;
    membership.member_count = sizeof(members) / sizeof(members[0]);
    plan_config.local_node_id = fixture.node.router.local_node_id;
    memcpy(plan_config.local_boot_id, fixture.node.router.local_boot_id,
           sizeof(plan_config.local_boot_id));
    plan_config.max_nodes = 8u;
    plan_config.max_endpoint_size = FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX;

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_start(node, UINT64_MAX), TURBO_OK);
    check_int_eq(
        flowie_cluster_node_topology_snapshot(node, current, 2u, &current_count, &revision),
        TURBO_OK);
    check_size_eq(current_count, 2u);
    check_uint_eq(revision, 1u);
    check_mem_eq(current[0].node_id.data, "node-b", current[0].node_id.len);
    check_mem_eq(current[1].node_id.data, "node-c", current[1].node_id.len);
    check_int_eq(fake_cluster.listener_authorize(
                     fake_cluster.listener_authorize_ctx, tstr_v_from_cstr("node-b"),
                     fixture.connector_peers[1].boot_id, fingerprints[0]),
                 TURBO_OK);
    plan_config.last_applied_revision = revision;
    check_int_eq(flowie_cluster_topology_plan_build(&plan_config, &membership, current,
                                                    current_count, &plan),
                 TURBO_OK);
    check_size_eq(flowie_cluster_topology_plan_operation_count(plan), 4u);

    fake_cluster.fail_connector_drain_once_id = (uint32_t)'c';
    check_int_eq(flowie_cluster_node_apply_topology(node, plan, UINT64_MAX), TURBO_EBUSY);
    check_int_eq(
        flowie_cluster_node_topology_snapshot(node, current, 2u, &current_count, &revision),
        TURBO_OK);
    check_size_eq(current_count, 1u);
    check_uint_eq(revision, 1u);
    check_mem_eq(current[0].node_id.data, "node-c", current[0].node_id.len);
    check_int_eq(fake_cluster.listener_authorize(
                     fake_cluster.listener_authorize_ctx, tstr_v_from_cstr("node-b"),
                     fixture.connector_peers[1].boot_id, fingerprints[0]),
                 TURBO_EPERM);
    check_int_eq(fake_cluster.listener_authorize(
                     fake_cluster.listener_authorize_ctx, tstr_v_from_cstr("node-c"),
                     fixture.connector_peers[0].boot_id, fingerprints[1]),
                 TURBO_OK);

    check_int_eq(flowie_cluster_node_apply_topology(node, plan, UINT64_MAX), TURBO_OK);
    check_int_eq(
        flowie_cluster_node_topology_snapshot(node, current, 2u, &current_count, &revision),
        TURBO_OK);
    check_size_eq(current_count, 2u);
    check_uint_eq(revision, 2u);
    check_mem_eq(current[0].node_id.data, "node-b", current[0].node_id.len);
    check_mem_eq(current[0].boot_id, boot_b, sizeof(boot_b));
    check_mem_eq(current[0].advertised_endpoint.data, "127.0.0.1:7202",
                 current[0].advertised_endpoint.len);
    check_mem_eq(current[1].node_id.data, "node-d", current[1].node_id.len);
    check_mem_eq(current[1].boot_id, boot_d, sizeof(boot_d));
    check_int_eq(fake_cluster.listener_authorize(fake_cluster.listener_authorize_ctx,
                                                 tstr_v_from_cstr("node-b"), boot_b,
                                                 fingerprints[0]),
                 TURBO_OK);
    check_int_eq(fake_cluster.listener_authorize(fake_cluster.listener_authorize_ctx,
                                                 tstr_v_from_cstr("node-d"), boot_d,
                                                 fingerprints[2]),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_apply_topology(node, plan, UINT64_MAX), TURBO_EPROTO);

    flowie_cluster_topology_plan_destroy(plan);
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }

  it("turns a connector start failure into a closed destroyable generation") {
    cluster_fixture_t fixture;
    flowie_cluster_node_t *node = NULL;
    cluster_fixture_init(&fixture);
    fake_cluster.fail_connector_start_id = (uint32_t)'c';

    check_int_eq(flowie_cluster_node_create_with_api(&fixture.node, &FAKE_CLUSTER_API, &node),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_register_edge(node, cluster_test_edge_receive, &fixture),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_start(node, UINT64_MAX), TURBO_EIO);
    check_int_eq(flowie_cluster_node_unregister_edge(node, cluster_test_edge_receive, &fixture),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_destroy(node), TURBO_OK);
    check_int_eq(fake_cluster.live_components, 0);
  }
}
