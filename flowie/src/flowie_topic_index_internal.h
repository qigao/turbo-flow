#ifndef FLOWIE_TOPIC_INDEX_INTERNAL_H
#define FLOWIE_TOPIC_INDEX_INTERNAL_H

#include "flowie_mqtt_protocol.h"
#include "turbo_flow.h"
#include "turbo_hash.h"
#include "turbo_str.h"
#include "turbo_vec.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** @internal Owner-lane-only binding for O(1) removal from a terminal bucket. */
typedef enum flowie_topic_bucket_e {
  FLOWIE_TOPIC_BUCKET_NONE = 0,
  FLOWIE_TOPIC_BUCKET_TERMINAL = 1,
  FLOWIE_TOPIC_BUCKET_HASH = 2
} flowie_topic_bucket_t;

typedef struct flowie_topic_node_s flowie_topic_node_t;

typedef struct flowie_topic_index_binding_s {
  flowie_topic_node_t *node;
  size_t position;
  flowie_topic_bucket_t bucket;
} flowie_topic_index_binding_t;

#define FLOWIE_TOPIC_INDEX_NO_ENTRY SIZE_MAX

/**
 * @internal
 * MQTT topic-filter trie. Exact child nodes own their map key token, allowing
 * filters to be removed without leaving borrowed storage behind. The owner
 * lane serializes mutation and matching; this derived index is not a
 * subscription fact source.
 */
struct flowie_topic_node_s {
  turbo_hash_map_t exact_children;
  turbo_vec_t terminal_entries;
  turbo_vec_t hash_entries;
  flowie_topic_node_t *plus_child;
  flowie_topic_node_t *parent;
  tstr_t token;
  size_t node_slot;
  int plus_edge;
};

typedef struct flowie_topic_index_s {
  flowie_topic_node_t *root;
  turbo_vec_t nodes;
  turbo_vec_t match_active;
  turbo_vec_t match_next;
  int initialized;
} flowie_topic_index_t;

typedef int (*flowie_topic_index_visit_fn)(void *ctx, size_t entry_index);

CXX_C_API int flowie_topic_index_init(flowie_topic_index_t *index);
CXX_C_API void flowie_topic_index_destroy(flowie_topic_index_t *index);
CXX_C_API int flowie_topic_index_insert(flowie_topic_index_t *index, flowie_mqtt_span_t filter,
                                        size_t entry_index);
CXX_C_API int flowie_topic_index_insert_bound(flowie_topic_index_t *index,
                                              flowie_mqtt_span_t filter, size_t entry_index,
                                              flowie_topic_index_binding_t *binding);
/**
 * Remove an entry using its stable binding. If swap-remove moves another entry,
 * `moved_entry_index` receives that entry so its binding position can be fixed.
 */
CXX_C_API int flowie_topic_index_remove(flowie_topic_index_t *index,
                                        flowie_topic_index_binding_t *binding, size_t entry_index,
                                        size_t *moved_entry_index);
/** Append matching entry indices to caller-owned `matched`; existing values remain. */
CXX_C_API int flowie_topic_index_match(flowie_topic_index_t *index, flowie_mqtt_span_t topic,
                                       turbo_vec_t *matched);
/**
 * Visit filters matching one concrete topic without shared mutable scratch. The index must remain
 * immutable for the duration of this call and may then be read concurrently.
 */
CXX_C_API int flowie_topic_index_visit_topic(const flowie_topic_index_t *index,
                                             flowie_mqtt_span_t topic,
                                             flowie_topic_index_visit_fn visit, void *ctx);
/** @internal Visit a Topic Name already accepted by the MQTT parser. */
CXX_C_API int flowie_topic_index_visit_validated_topic(const flowie_topic_index_t *index,
                                                       flowie_mqtt_span_t topic,
                                                       flowie_topic_index_visit_fn visit,
                                                       void *ctx);
/** Visit indexed policy filters whose topic language contains all of `requested_filter`. */
CXX_C_API int flowie_topic_index_visit_containing_filters(const flowie_topic_index_t *index,
                                                          flowie_mqtt_span_t requested_filter,
                                                          flowie_topic_index_visit_fn visit,
                                                          void *ctx);
/** @internal Visit a Topic Filter already accepted by the MQTT parser. */
CXX_C_API int
flowie_topic_index_visit_validated_containing_filters(const flowie_topic_index_t *index,
                                                      flowie_mqtt_span_t requested_filter,
                                                      flowie_topic_index_visit_fn visit, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_TOPIC_INDEX_INTERNAL_H */
