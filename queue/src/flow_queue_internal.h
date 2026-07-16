#ifndef FLOW_QUEUE_INTERNAL_H
#define FLOW_QUEUE_INTERNAL_H

#include "turbo_flow_queue.h"

int flow_queue_bind_channel(turbo_flow_queue_t *queue, const char *channel_name);
int flow_queue_channel_matches(const turbo_flow_queue_t *queue, const char *channel_name);

#endif /* FLOW_QUEUE_INTERNAL_H */
