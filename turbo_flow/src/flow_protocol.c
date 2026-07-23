#include "flow_internal.h"

#include <string.h>

static int flow_protocol_settlement_point_valid(turbo_flow_protocol_settlement_point_t point) {
  return point >= TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED &&
         point <= TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
}

static int flow_pattern_selector_valid(const turbo_flow_pattern_selector_t *selector) {
  return selector && selector->size >= sizeof(*selector) &&
         selector->contract_version == TURBO_FLOW_PROTOCOL_CONTRACT_VERSION;
}

static int flow_pattern_iterator_valid(
    const turbo_flow_pattern_selection_iterator_t *iterator) {
  return iterator && iterator->size >= sizeof(*iterator) &&
         iterator->contract_version == TURBO_FLOW_PROTOCOL_CONTRACT_VERSION &&
         (iterator->selection == TURBO_FLOW_PATTERN_SELECT_FAN_OUT ||
          iterator->selection == TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN) &&
         iterator->candidate_count != 0u && iterator->remaining <= iterator->candidate_count &&
         iterator->next_offset < iterator->candidate_count;
}

static int flow_pattern_exchange_valid(const turbo_flow_pattern_exchange_t *exchange) {
  return exchange && exchange->size >= sizeof(*exchange) &&
         exchange->contract_version == TURBO_FLOW_PROTOCOL_CONTRACT_VERSION;
}

static int flow_pattern_exchange_active_state(turbo_flow_pattern_exchange_state_t state) {
  return state == TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY ||
         state == TURBO_FLOW_PATTERN_EXCHANGE_PROCESSING_REQUEST;
}

int turbo_flow_pattern_role_validate(turbo_flow_pattern_role_t role) {
  return role >= TURBO_FLOW_PATTERN_PUBLISH && role <= TURBO_FLOW_PATTERN_EXTENDED_SUBSCRIBE
             ? TURBO_OK
             : TURBO_EINVAL;
}

int turbo_flow_pattern_roles_compatible(turbo_flow_pattern_role_t local,
                                        turbo_flow_pattern_role_t remote) {
  if (turbo_flow_pattern_role_validate(local) != TURBO_OK ||
      turbo_flow_pattern_role_validate(remote) != TURBO_OK)
    return 0;
  switch (local) {
  case TURBO_FLOW_PATTERN_PUBLISH:
    return remote == TURBO_FLOW_PATTERN_SUBSCRIBE ||
           remote == TURBO_FLOW_PATTERN_EXTENDED_SUBSCRIBE;
  case TURBO_FLOW_PATTERN_SUBSCRIBE:
    return remote == TURBO_FLOW_PATTERN_PUBLISH ||
           remote == TURBO_FLOW_PATTERN_EXTENDED_PUBLISH;
  case TURBO_FLOW_PATTERN_PUSH:
    return remote == TURBO_FLOW_PATTERN_PULL;
  case TURBO_FLOW_PATTERN_PULL:
    return remote == TURBO_FLOW_PATTERN_PUSH;
  case TURBO_FLOW_PATTERN_ROUTE:
    return remote == TURBO_FLOW_PATTERN_DEAL;
  case TURBO_FLOW_PATTERN_DEAL:
    return remote == TURBO_FLOW_PATTERN_ROUTE;
  case TURBO_FLOW_PATTERN_PAIR:
    return remote == TURBO_FLOW_PATTERN_PAIR;
  case TURBO_FLOW_PATTERN_REQUEST:
    return remote == TURBO_FLOW_PATTERN_REPLY;
  case TURBO_FLOW_PATTERN_REPLY:
    return remote == TURBO_FLOW_PATTERN_REQUEST;
  case TURBO_FLOW_PATTERN_EXTENDED_PUBLISH:
    return remote == TURBO_FLOW_PATTERN_SUBSCRIBE ||
           remote == TURBO_FLOW_PATTERN_EXTENDED_SUBSCRIBE;
  case TURBO_FLOW_PATTERN_EXTENDED_SUBSCRIBE:
    return remote == TURBO_FLOW_PATTERN_PUBLISH ||
           remote == TURBO_FLOW_PATTERN_EXTENDED_PUBLISH;
  default:
    return 0;
  }
}

int turbo_flow_pattern_selector_init(turbo_flow_pattern_selector_t *selector) {
  if (!selector) return TURBO_EINVAL;
  selector->size = sizeof(*selector);
  selector->contract_version = TURBO_FLOW_PROTOCOL_CONTRACT_VERSION;
  atomic_init(&selector->cursor, 0u);
  return TURBO_OK;
}

int turbo_flow_pattern_selection_begin(
    turbo_flow_pattern_selector_t *selector, turbo_flow_pattern_selection_t selection,
    size_t candidate_count, turbo_flow_pattern_selection_iterator_t *iterator) {
  turbo_flow_pattern_selection_iterator_t value = TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  uint_fast64_t ticket = 0u;
  if (!flow_pattern_selector_valid(selector) || !iterator ||
      iterator->size < sizeof(*iterator) ||
      iterator->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION ||
      (selection != TURBO_FLOW_PATTERN_SELECT_FAN_OUT &&
       selection != TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN))
    return TURBO_EINVAL;
  if (candidate_count == 0u) return TURBO_ENOENT;
  if (selection == TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN)
    ticket = atomic_fetch_add_explicit(&selector->cursor, 1u, memory_order_relaxed);
  value.selection = selection;
  value.candidate_count = candidate_count;
  value.next_offset = selection == TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN
                          ? (size_t)(ticket % candidate_count)
                          : 0u;
  value.remaining = candidate_count;
  *iterator = value;
  return TURBO_OK;
}

int turbo_flow_pattern_selection_next(turbo_flow_pattern_selection_iterator_t *iterator,
                                      size_t *candidate_index) {
  if (!flow_pattern_iterator_valid(iterator) || !candidate_index) return TURBO_EINVAL;
  if (iterator->remaining == 0u) return TURBO_ENOENT;
  *candidate_index = iterator->next_offset;
  iterator->next_offset += 1u;
  if (iterator->next_offset == iterator->candidate_count) iterator->next_offset = 0u;
  iterator->remaining -= 1u;
  return TURBO_OK;
}

int turbo_flow_pattern_route_validate(const turbo_flow_pattern_route_t *route) {
  return route && route->size >= sizeof(*route) &&
                 route->contract_version == TURBO_FLOW_PROTOCOL_CONTRACT_VERSION &&
                 route->route_id != 0u && route->generation != 0u
             ? TURBO_OK
             : TURBO_EINVAL;
}

int turbo_flow_pattern_routes_match(const turbo_flow_pattern_route_t *requested,
                                    const turbo_flow_pattern_route_t *candidate) {
  return turbo_flow_pattern_route_validate(requested) == TURBO_OK &&
         turbo_flow_pattern_route_validate(candidate) == TURBO_OK &&
         requested->route_id == candidate->route_id &&
         requested->generation == candidate->generation;
}

int turbo_flow_pattern_exchange_init(turbo_flow_pattern_exchange_t *exchange) {
  if (!exchange) return TURBO_EINVAL;
  exchange->size = sizeof(*exchange);
  exchange->contract_version = TURBO_FLOW_PROTOCOL_CONTRACT_VERSION;
  atomic_init(&exchange->state, TURBO_FLOW_PATTERN_EXCHANGE_READY);
  atomic_init(&exchange->correlation_id, 0u);
  return TURBO_OK;
}

int turbo_flow_pattern_exchange_snapshot(const turbo_flow_pattern_exchange_t *exchange,
                                         turbo_flow_pattern_exchange_state_t *state,
                                         uint64_t *correlation_id) {
  int value;
  if (!flow_pattern_exchange_valid(exchange) || !state || !correlation_id) return TURBO_EINVAL;
  value = atomic_load_explicit(&exchange->state, memory_order_acquire);
  if (value < TURBO_FLOW_PATTERN_EXCHANGE_READY ||
      value > TURBO_FLOW_PATTERN_EXCHANGE_RESETTING)
    return TURBO_EPROTO;
  *state = (turbo_flow_pattern_exchange_state_t)value;
  *correlation_id = atomic_load_explicit(&exchange->correlation_id, memory_order_acquire);
  return TURBO_OK;
}

int turbo_flow_pattern_exchange_begin(turbo_flow_pattern_exchange_t *exchange,
                                      turbo_flow_pattern_exchange_state_t active_state,
                                      uint64_t correlation_id) {
  uint_fast64_t empty = 0u;
  int expected = TURBO_FLOW_PATTERN_EXCHANGE_READY;
  if (!flow_pattern_exchange_valid(exchange) || !flow_pattern_exchange_active_state(active_state) ||
      correlation_id == 0u)
    return TURBO_EINVAL;
  if (!atomic_compare_exchange_strong_explicit(&exchange->correlation_id, &empty, correlation_id,
                                               memory_order_acq_rel, memory_order_acquire))
    return TURBO_EBUSY;
  if (atomic_compare_exchange_strong_explicit(&exchange->state, &expected, active_state,
                                              memory_order_release, memory_order_acquire))
    return TURBO_OK;
  atomic_store_explicit(&exchange->correlation_id, 0u, memory_order_release);
  return TURBO_EBUSY;
}

int turbo_flow_pattern_exchange_match(const turbo_flow_pattern_exchange_t *exchange,
                                      turbo_flow_pattern_exchange_state_t expected_state,
                                      uint64_t correlation_id) {
  if (!flow_pattern_exchange_valid(exchange) || !flow_pattern_exchange_active_state(expected_state) ||
      correlation_id == 0u)
    return TURBO_EINVAL;
  if (atomic_load_explicit(&exchange->state, memory_order_acquire) != (int)expected_state)
    return TURBO_EBUSY;
  return atomic_load_explicit(&exchange->correlation_id, memory_order_acquire) == correlation_id
             ? TURBO_OK
             : TURBO_EPROTO;
}

int turbo_flow_pattern_exchange_finish(turbo_flow_pattern_exchange_t *exchange,
                                       turbo_flow_pattern_exchange_state_t expected_state,
                                       uint64_t correlation_id,
                                       turbo_flow_pattern_exchange_state_t terminal_state) {
  int rc;
  if (terminal_state != TURBO_FLOW_PATTERN_EXCHANGE_READY &&
      terminal_state != TURBO_FLOW_PATTERN_EXCHANGE_RESETTING)
    return TURBO_EINVAL;
  rc = turbo_flow_pattern_exchange_match(exchange, expected_state, correlation_id);
  if (rc != TURBO_OK) return rc;
  if (terminal_state == TURBO_FLOW_PATTERN_EXCHANGE_READY)
    atomic_store_explicit(&exchange->correlation_id, 0u, memory_order_release);
  atomic_store_explicit(&exchange->state, terminal_state, memory_order_release);
  return TURBO_OK;
}

int turbo_flow_pattern_exchange_mark_resetting(turbo_flow_pattern_exchange_t *exchange) {
  int state;
  if (!flow_pattern_exchange_valid(exchange)) return TURBO_EINVAL;
  state = atomic_load_explicit(&exchange->state, memory_order_acquire);
  if (!flow_pattern_exchange_active_state((turbo_flow_pattern_exchange_state_t)state) &&
      state != TURBO_FLOW_PATTERN_EXCHANGE_RESETTING)
    return TURBO_EBUSY;
  atomic_store_explicit(&exchange->state, TURBO_FLOW_PATTERN_EXCHANGE_RESETTING,
                        memory_order_release);
  return TURBO_OK;
}

int turbo_flow_pattern_exchange_reset(turbo_flow_pattern_exchange_t *exchange) {
  if (!flow_pattern_exchange_valid(exchange)) return TURBO_EINVAL;
  atomic_store_explicit(&exchange->correlation_id, 0u, memory_order_release);
  atomic_store_explicit(&exchange->state, TURBO_FLOW_PATTERN_EXCHANGE_READY, memory_order_release);
  return TURBO_OK;
}

int turbo_flow_fmq_pattern_validate(turbo_flow_fmq_pattern_t pattern) {
  return pattern >= TURBO_FLOW_FMQ_PUB && pattern <= TURBO_FLOW_FMQ_XSUB ? TURBO_OK
                                                                         : TURBO_EINVAL;
}

int turbo_flow_fmq_patterns_compatible(turbo_flow_fmq_pattern_t local,
                                       turbo_flow_fmq_pattern_t remote) {
  if (turbo_flow_fmq_pattern_validate(local) != TURBO_OK ||
      turbo_flow_fmq_pattern_validate(remote) != TURBO_OK) {
    return 0;
  }
  return turbo_flow_pattern_roles_compatible((turbo_flow_pattern_role_t)local,
                                             (turbo_flow_pattern_role_t)remote);
}

int turbo_flow_protocol_message_validate(const turbo_flow_protocol_message_t *message) {
  if (!message || message->size < sizeof(*message) ||
      message->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION ||
      message->protocol < TURBO_FLOW_PROTOCOL_FMQ || message->protocol > TURBO_FLOW_PROTOCOL_MQTT ||
      message->protocol_version == 0u || message->kind < TURBO_FLOW_PROTOCOL_MESSAGE_DATA ||
      message->kind > TURBO_FLOW_PROTOCOL_MESSAGE_ACK || message->duplicate > 1u ||
      message->retain > 1u || message->session_generation == 0u) {
    return TURBO_EINVAL;
  }
  if (message->protocol == TURBO_FLOW_PROTOCOL_MQTT) {
    if ((message->protocol_version != TURBO_FLOW_MQTT_PROTOCOL_3_1 &&
         message->protocol_version != TURBO_FLOW_MQTT_PROTOCOL_3_1_1 &&
         message->protocol_version != TURBO_FLOW_MQTT_PROTOCOL_5_0) ||
        message->qos > TURBO_FLOW_PROTOCOL_QOS_2 ||
        (message->qos == TURBO_FLOW_PROTOCOL_QOS_0 && message->packet_id != 0u) ||
        (message->qos > TURBO_FLOW_PROTOCOL_QOS_0 && message->packet_id == 0u)) {
      return TURBO_EPROTO;
    }
  } else if (message->qos != TURBO_FLOW_PROTOCOL_QOS_0) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_protocol_origin_validate(const turbo_flow_protocol_origin_t *origin) {
  if (!origin || origin->size < sizeof(*origin) ||
      origin->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION ||
      origin->protocol < TURBO_FLOW_PROTOCOL_FMQ || origin->protocol > TURBO_FLOW_PROTOCOL_MQTT ||
      origin->protocol_version == 0u || origin->session_id == 0u) {
    return TURBO_EINVAL;
  }
  if (origin->protocol == TURBO_FLOW_PROTOCOL_MQTT &&
      origin->protocol_version != TURBO_FLOW_MQTT_PROTOCOL_3_1 &&
      origin->protocol_version != TURBO_FLOW_MQTT_PROTOCOL_3_1_1 &&
      origin->protocol_version != TURBO_FLOW_MQTT_PROTOCOL_5_0) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_protocol_settlement_policy_validate(
    const turbo_flow_protocol_settlement_policy_t *policy) {
  if (!policy || policy->size < sizeof(*policy) ||
      !flow_protocol_settlement_point_valid(policy->qos0) ||
      !flow_protocol_settlement_point_valid(policy->qos1) ||
      !flow_protocol_settlement_point_valid(policy->qos2)) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int turbo_flow_protocol_owner_settle(
    const turbo_flow_protocol_owner_ops_t *owner, void *ctx,
    const turbo_flow_protocol_settlement_policy_t *policy,
    const turbo_flow_protocol_settlement_request_t *request) {
  turbo_flow_protocol_settlement_point_t required;
  int rc;
  if (!owner || owner->size < sizeof(*owner) || !owner->settle || !request ||
      request->size < sizeof(*request) || request->attempt == 0u ||
      !flow_protocol_settlement_point_valid(request->point)) {
    return TURBO_EINVAL;
  }
  rc = turbo_flow_protocol_message_validate(&request->message);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_protocol_settlement_policy_validate(policy);
  if (rc != TURBO_OK) return rc;
  required = request->message.qos == TURBO_FLOW_PROTOCOL_QOS_0 ? policy->qos0
             : request->message.qos == TURBO_FLOW_PROTOCOL_QOS_1 ? policy->qos1
                                                                 : policy->qos2;
  if (request->point != required) return TURBO_EBUSY;
  return owner->settle(ctx, request);
}

static flow_protocol_route_owner_key_t
flow_protocol_route_owner_key(turbo_flow_protocol_id_t protocol, uint64_t owner_instance_id) {
  flow_protocol_route_owner_key_t key;
  memset(&key, 0, sizeof(key));
  key.protocol = (uint32_t)protocol;
  key.owner_instance_id = owner_instance_id;
  return key;
}

int turbo_flow_register_protocol_route_owner(
    turbo_flow_t *flow, turbo_flow_protocol_id_t protocol, uint64_t owner_instance_id,
    const turbo_flow_protocol_route_owner_ops_t *ops, void *ctx) {
  flow_protocol_route_owner_key_t key;
  flow_protocol_route_owner_t owner;
  if (!flow || protocol < TURBO_FLOW_PROTOCOL_FMQ || protocol > TURBO_FLOW_PROTOCOL_MQTT ||
      owner_instance_id == 0u || !ops || ops->size < sizeof(*ops) || !ops->settle)
    return TURBO_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) return TURBO_EBUSY;
  key = flow_protocol_route_owner_key(protocol, owner_instance_id);
  if (turbo_hash_map_get_const(&flow->protocol_route_owners, &key)) return TURBO_EALREADY;
  owner.ops = *ops;
  owner.ops.size = sizeof(owner.ops);
  owner.ctx = ctx;
  return turbo_hash_map_put(&flow->protocol_route_owners, &key, &owner);
}

int turbo_flow_unregister_protocol_route_owner(turbo_flow_t *flow,
                                                turbo_flow_protocol_id_t protocol,
                                                uint64_t owner_instance_id) {
  flow_protocol_route_owner_key_t key;
  if (!flow || protocol < TURBO_FLOW_PROTOCOL_FMQ || protocol > TURBO_FLOW_PROTOCOL_MQTT ||
      owner_instance_id == 0u)
    return TURBO_EINVAL;
  if (flow->state == TURBO_FLOW_STATE_STARTED) return TURBO_EBUSY;
  key = flow_protocol_route_owner_key(protocol, owner_instance_id);
  return turbo_hash_map_remove(&flow->protocol_route_owners, &key, NULL);
}

int turbo_flow_protocol_route_settle(
    turbo_flow_t *flow, const turbo_flow_protocol_route_t *route,
    const turbo_flow_protocol_settlement_request_t *request) {
  flow_protocol_route_owner_key_t key;
  const flow_protocol_route_owner_t *owner;
  if (!flow || !route || route->size < sizeof(*route) ||
      route->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION || route->reserved != 0u ||
      route->protocol < TURBO_FLOW_PROTOCOL_FMQ || route->protocol > TURBO_FLOW_PROTOCOL_MQTT ||
      route->owner_instance_id == 0u || route->session_id == 0u ||
      route->session_generation == 0u || !request || request->size < sizeof(*request) ||
      request->attempt == 0u || request->status != TURBO_OK ||
      request->point < TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED ||
      request->point > TURBO_FLOW_PROTOCOL_SETTLE_DURABLE)
    return TURBO_EINVAL;
  if (turbo_flow_protocol_message_validate(&request->message) != TURBO_OK ||
      request->message.protocol != route->protocol ||
      request->message.session_generation != route->session_generation)
    return TURBO_EPROTO;
  key = flow_protocol_route_owner_key(route->protocol, route->owner_instance_id);
  owner = (const flow_protocol_route_owner_t *)
      turbo_hash_map_get_const(&flow->protocol_route_owners, &key);
  if (!owner) return TURBO_ENOENT;
  return owner->ops.settle(owner->ctx, route, request);
}
