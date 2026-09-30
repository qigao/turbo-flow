#ifndef FLOW_DATABIND_INTERNAL_H
#define FLOW_DATABIND_INTERNAL_H

#include "flow_internal.h"

#include <data_bind_native_binding.h>

struct flow_databind_source_binding_s {
  int bound;
  flow_databind_transport_kind_t transport;
  DataBindFormat format;
  const void *transport_plan;
  tstr channel_name;
  tstr message_type;
  DataBindNativeTypeBinding native;
};

typedef struct flow_databind_channel_plan_s {
  const char *channel_name;
  const char *message_type;
  const char *data_stable_id;
  DataBindNativeTypeBinding native;
} flow_databind_channel_plan_t;

#endif /* FLOW_DATABIND_INTERNAL_H */
