#include "turbo_flow_email.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SMTP4DEV_POP3_WAIT_ITERATIONS 5000u

typedef struct smtp4dev_pop3_capture_s {
  atomic_int received;
  size_t payload_len;
} smtp4dev_pop3_capture_t;

static int smtp4dev_pop3_capture(turbo_flow_msg_t *msg, void *ctx) {
  smtp4dev_pop3_capture_t *capture = (smtp4dev_pop3_capture_t *)ctx;
  if (!capture || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  capture->payload_len = msg->payload.len;
  atomic_store_explicit(&capture->received, 1, memory_order_release);
  return TURBO_OK;
}

static const char *env_or_default(const char *name, const char *fallback) {
  const char *value = getenv(name);
  return value && value[0] != '\0' ? value : fallback;
}

static int parse_port(const char *text, int *port) {
  char *end = NULL;
  long value;
  if (!text || !port) return TURBO_EINVAL;
  value = strtol(text, &end, 10);
  if (!end || *end != '\0' || value < 1 || value > 65535) return TURBO_EINVAL;
  *port = (int)value;
  return TURBO_OK;
}

int main(void) {
  static const char *dsl = "source inbox adapter smtp4dev.pop3\n"
                           "stage capture\n"
                           "stage main {\n"
                           "  inbox -> capture\n"
                           "}\n";
  turbo_flow_email_pop3_config_t config = {0};
  smtp4dev_pop3_capture_t capture;
  turbo_flow_t *flow = NULL;
  int rc;

  atomic_init(&capture.received, 0);
  capture.payload_len = 0;
  config.host = env_or_default("TURBO_FLOW_SMTP4DEV_HOST", "127.0.0.1");
  config.username = env_or_default("TURBO_FLOW_SMTP4DEV_POP3_USERNAME", "turbo");
  config.password = env_or_default("TURBO_FLOW_SMTP4DEV_POP3_PASSWORD", "turbo");
  config.timeout_ms = 2000;
  config.poll_interval_ms = 1000;
  rc = parse_port(env_or_default("TURBO_FLOW_SMTP4DEV_POP3_PORT", "110"), &config.port);
  if (rc != TURBO_OK) return 2;

  flow = turbo_flow_create();
  if (!flow) return 3;
  rc = turbo_flow_email_register_pop3_source_adapter(flow, "smtp4dev.pop3", &config);
  if (rc == TURBO_OK) {
    rc = turbo_flow_register_stage_ex(flow, "capture", smtp4dev_pop3_capture, &capture, NULL);
  }
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(flow, dsl, strlen(dsl));
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  if (rc == TURBO_OK) {
    for (uint32_t i = 0;
         i < SMTP4DEV_POP3_WAIT_ITERATIONS &&
         !atomic_load_explicit(&capture.received, memory_order_acquire);
         ++i) {
      turbo_sleep_ms(1);
    }
    if (!atomic_load_explicit(&capture.received, memory_order_acquire)) rc = TURBO_ETIMEDOUT;
  }
  if (turbo_flow_state(flow) == TURBO_FLOW_STATE_STARTED) (void)turbo_flow_stop(flow);
  turbo_flow_destroy(flow);
  if (rc != TURBO_OK) {
    fprintf(stderr, "smtp4dev POP3 integration failed: %d\n", rc);
    return 1;
  }
  printf("received %zu raw bytes via pop3://%s:%d\n", capture.payload_len, config.host,
         config.port);
  return 0;
}
