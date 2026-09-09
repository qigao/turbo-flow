#ifndef FLOW_PLUGIN_PROJECTION_FIXTURE_H
#define FLOW_PLUGIN_PROJECTION_FIXTURE_H
#include "turbo_flow_plugin_operation.h"
#include <salts/thread.h>

enum { PROJECTION_TEST_VALUE = 73, PROJECTION_TEST_CAPACITY = 4,
       PROJECTION_TEST_EVENTS = 32, PROJECTION_PAYLOAD_DESTROY = 100,
       PROJECTION_CONTEXT_RELEASE = 101 };
/* 仅测试协议：存储由测试拥有并存活到 host 卸载；所有共享访问持同一 mutex。 */
typedef struct projection_observer_s {
  salts_mutex_t mutex;
  salts_cond_t cond;
  int events[PROJECTION_TEST_EVENTS];
  size_t count;
  int clone_error;
  int release_busy;
  int release_attempts;
  int block_clone;
  int block_destroy;
  int entered;
  int proceed;
} projection_observer_t;

typedef struct projection_fixture_protocol_s {
  int (*create)(projection_observer_t *observer,
                turbo_flow_projection_owner_config_t *config, void **value);
} projection_fixture_protocol_t;
#endif
