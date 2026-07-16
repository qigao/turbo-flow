#include "flowie_topic_index_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static flowie_mqtt_span_t flowie_topic_test_span(const char *value) {
  return (flowie_mqtt_span_t){(const uint8_t *)value, strlen(value)};
}

static int flowie_topic_test_contains(const turbo_vec_t *matches, size_t expected) {
  for (size_t i = 0u; i < turbo_vec_size(matches); ++i) {
    const size_t *value = (const size_t *)turbo_vec_at_const(matches, i);
    if (value && *value == expected) return 1;
  }
  return 0;
}

spec("flowie topic index") {
  it("indexes exact wildcard shared and system topic filters") {
    static const char *const filters[] = {
        "sensors/+/temp", "sensors/#", "sensors/a/temp",
        "$SYS/#",         "#",         "$share/workers/sensors/+/temp"};
    flowie_topic_index_t index;
    turbo_vec_t matches;
    memset(&index, 0, sizeof(index));
    check_int_eq(flowie_topic_index_init(&index), TURBO_OK);
    check_int_eq(turbo_vec_init(&matches, sizeof(size_t)), TURBO_OK);
    for (size_t i = 0u; i < sizeof(filters) / sizeof(filters[0]); ++i) {
      check_int_eq(flowie_topic_index_insert(&index, flowie_topic_test_span(filters[i]), i),
                   TURBO_OK);
    }

    check_int_eq(
        flowie_topic_index_match(&index, flowie_topic_test_span("sensors/a/temp"), &matches),
        TURBO_OK);
    check_size_eq(turbo_vec_size(&matches), 5u);
    check_true(flowie_topic_test_contains(&matches, 0u));
    check_true(flowie_topic_test_contains(&matches, 1u));
    check_true(flowie_topic_test_contains(&matches, 2u));
    check_true(flowie_topic_test_contains(&matches, 4u));
    check_true(flowie_topic_test_contains(&matches, 5u));
    check_false(flowie_topic_test_contains(&matches, 3u));

    turbo_vec_clear(&matches);
    check_int_eq(flowie_topic_index_match(&index, flowie_topic_test_span("$SYS/status"), &matches),
                 TURBO_OK);
    check_size_eq(turbo_vec_size(&matches), 1u);
    check_true(flowie_topic_test_contains(&matches, 3u));
    check_false(flowie_topic_test_contains(&matches, 4u));

    turbo_vec_destroy(&matches);
    flowie_topic_index_destroy(&index);
  }

  it("rejects malformed filters and invalid arguments") {
    flowie_topic_index_t index;
    turbo_vec_t matches;
    memset(&index, 0, sizeof(index));
    check_int_eq(flowie_topic_index_init(&index), TURBO_OK);
    check_int_eq(turbo_vec_init(&matches, sizeof(size_t)), TURBO_OK);
    check_int_eq(flowie_topic_index_insert(&index, flowie_topic_test_span("a/#/b"), 1u),
                 TURBO_EPROTO);
    check_int_eq(flowie_topic_index_insert(&index, flowie_topic_test_span("$share/group"), 2u),
                 TURBO_EPROTO);
    check_int_eq(flowie_topic_index_match(&index, (flowie_mqtt_span_t){NULL, 0u}, &matches),
                 TURBO_EINVAL);
    turbo_vec_destroy(&matches);
    flowie_topic_index_destroy(&index);
  }

  it("removes bound entries updates swapped positions and prunes empty branches") {
    flowie_topic_index_t index;
    flowie_topic_index_binding_t bindings[4];
    turbo_vec_t matches;
    size_t moved = FLOWIE_TOPIC_INDEX_NO_ENTRY;
    size_t removed_position;
    memset(&index, 0, sizeof(index));
    memset(bindings, 0, sizeof(bindings));
    check_int_eq(flowie_topic_index_init(&index), TURBO_OK);
    check_int_eq(turbo_vec_init(&matches, sizeof(size_t)), TURBO_OK);
    check_int_eq(flowie_topic_index_insert_bound(&index, flowie_topic_test_span("devices/a"), 0u,
                                                 &bindings[0]),
                 TURBO_OK);
    check_int_eq(flowie_topic_index_insert_bound(&index, flowie_topic_test_span("devices/a"), 1u,
                                                 &bindings[1]),
                 TURBO_OK);
    check_int_eq(flowie_topic_index_insert_bound(&index, flowie_topic_test_span("devices/+"), 2u,
                                                 &bindings[2]),
                 TURBO_OK);
    check_int_eq(flowie_topic_index_insert_bound(
                     &index, flowie_topic_test_span("$share/workers/devices/#"), 3u, &bindings[3]),
                 TURBO_OK);

    removed_position = bindings[0].position;
    check_int_eq(flowie_topic_index_remove(&index, &bindings[0], 0u, &moved), TURBO_OK);
    check_size_eq(moved, 1u);
    bindings[moved].position = removed_position;
    check_int_eq(flowie_topic_index_match(&index, flowie_topic_test_span("devices/a"), &matches),
                 TURBO_OK);
    check_size_eq(turbo_vec_size(&matches), 3u);
    check_true(flowie_topic_test_contains(&matches, 1u));
    check_true(flowie_topic_test_contains(&matches, 2u));
    check_true(flowie_topic_test_contains(&matches, 3u));

    turbo_vec_clear(&matches);
    check_int_eq(flowie_topic_index_remove(&index, &bindings[1], 1u, &moved), TURBO_OK);
    check_size_eq(moved, FLOWIE_TOPIC_INDEX_NO_ENTRY);
    check_int_eq(flowie_topic_index_remove(&index, &bindings[2], 2u, &moved), TURBO_OK);
    check_int_eq(flowie_topic_index_remove(&index, &bindings[3], 3u, &moved), TURBO_OK);
    check_int_eq(flowie_topic_index_match(&index, flowie_topic_test_span("devices/a"), &matches),
                 TURBO_OK);
    check_size_eq(turbo_vec_size(&matches), 0u);
    check_size_eq(turbo_vec_size(&index.nodes), 1u);

    check_int_eq(flowie_topic_index_insert_bound(&index, flowie_topic_test_span("devices/a"), 0u,
                                                 &bindings[0]),
                 TURBO_OK);
    check_int_eq(flowie_topic_index_match(&index, flowie_topic_test_span("devices/a"), &matches),
                 TURBO_OK);
    check_size_eq(turbo_vec_size(&matches), 1u);
    check_true(flowie_topic_test_contains(&matches, 0u));
    check_int_eq(flowie_topic_index_remove(&index, &bindings[0], 0u, &moved), TURBO_OK);
    check_size_eq(turbo_vec_size(&index.nodes), 1u);

    turbo_vec_destroy(&matches);
    flowie_topic_index_destroy(&index);
  }
}
